/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.apack.xdcr.task.TaskUtil;
import org.elasticsearch.apack.xdcr.task.index.IndexSyncTaskParams;
import org.elasticsearch.apack.xdcr.utils.Actions;
import org.elasticsearch.apack.xdcr.utils.RemoteConnections;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.apack.xdcr.action.index.PutFollowJobAction.*;
import static org.elasticsearch.apack.xdcr.utils.Ssl.remoteClient;

/**
 * 创建索引同步任务
 */
public class PutFollowJobAction extends Action<Request, Response, Builder> {

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {
        private String repository;
        private String index;

        public Request() {
        }

        public Request(String repository, String index) {
            this.repository = repository;
            this.index = index;
        }

        public String repository() {
            return repository;
        }

        public String index() {
            return index;
        }

        @Override
        public ActionRequestValidationException validate() {
            return Actions.notNull("repository", repository, "index", index);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            repository = in.readString();
            index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(index);
        }

        public static class Builder extends ActionRequestBuilder<Request, Response, Request.Builder> {
            protected Builder(ElasticsearchClient client, Action<Request, Response, Request.Builder> action, Request request) {
                super(client, action, request);
            }
        }
    }

    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<Request, Response, Builder> {
        protected Builder(ElasticsearchClient client, Action<Request, Response, Builder> action, Request request) {
            super(client, action, request);
        }
    }

    /**
     * Response
     */
    public static class Response extends ActionResponse implements ToXContentObject {

        private String[] indices;

        Response() {
        }

        public Response(String[] indices) {
            this.indices = indices;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array("followed", indices);
            builder.endObject();
            return builder;
        }
    }

    public static final PutFollowJobAction INSTANCE = new PutFollowJobAction();
    public static final String NAME = "cluster:admin/xdcr/index/sync/start";

    private PutFollowJobAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final Client client;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.client = client;
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }


        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            String repository = request.repository();
            String index = request.index();
            Map<String, String> headers = threadPool.getThreadContext().getHeaders();
            if (RemoteConnections.isConnected(client, request.repository()) == false) {
                throw new RemoteTransportException("remote cluster [" + repository + "] not connected", null);
            }

            List<String> existedIndices = TaskUtil.indices(state, repository);

            Client remoteClient = remoteClient(client, repository);
            remoteClient.admin().cluster().prepareState().clear().setMetaData(true).setIndices(index)
                    .execute(ActionListener.wrap(
                            r -> {
                                ClusterState remoteState = r.getState();
                                ImmutableOpenMap<String, IndexMetaData> indicesMetaData = remoteState.metaData().indices();
                                if (indicesMetaData.isEmpty()) {
                                    listener.onResponse(new Response());
                                    return;
                                }
                                indicesMetaData.forEach(cursor -> {
                                    String indexName = cursor.value.getIndex().getName();
                                    String indexUUID = cursor.value.getIndexUUID();
                                    if (existedIndices.contains(indexName) == false) {
                                        persistentTasksService.sendStartRequest(
                                                TaskIds.indexSyncTaskName(repository, indexName),
                                                IndexSyncTaskParams.NAME,
                                                new IndexSyncTaskParams(
                                                        repository,
                                                        indexName,
                                                        indexUUID,
                                                        headers),
                                                ActionListener.wrap(
                                                        taskResponse -> logger.info("follower job added {} ", taskResponse),
                                                        e -> logger.warn(" follower job add failed", e)
                                                )
                                        );
                                    }
                                });
                                String[] indices = indicesMetaData.keys().toArray(String.class);
                                listener.onResponse(new Response(indices));
                            },
                            e -> listener.onFailure(e)
                    ));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }
    }

}
