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
import org.elasticsearch.apack.xdcr.task.index.IndexSyncTaskParams;
import org.elasticsearch.apack.xdcr.utils.Actions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.apack.xdcr.action.index.DeleteFollowJobAction.*;

/**
 * 创建索引同步任务
 */
public class DeleteFollowJobAction extends Action<Request, Response, Builder> {

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

        public static class Builder extends ActionRequestBuilder<Request, Response, Builder> {
            protected Builder(ElasticsearchClient client, Action<Request, Response, Builder> action, Request request) {
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
            this.indices = new String[]{};
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
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.array("unfollowed", indices);
            builder.endObject();
            return builder;
        }
    }

    public static final DeleteFollowJobAction INSTANCE = new DeleteFollowJobAction();
    public static final String NAME = "cluster:admin/xdcr/index/sync/stop";

    private DeleteFollowJobAction() {
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

        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
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
            String pattern = request.index();

            Map<String, PersistentTasksCustomMetaData.PersistentTask<?>> taskMap = PersistentTasksCustomMetaData
                    .builder(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE)).build().taskMap();

            List<String> followIndices = new ArrayList<>();
            taskMap.forEach((k, v) -> {
                if (k.startsWith(TaskIds.SYNC_INDEX_TASKNAME_PREFIX)) {
                    IndexSyncTaskParams params = (IndexSyncTaskParams) v.getParams();
                    String indexName = params.index();
                    if (Regex.simpleMatch(pattern, indexName)) {
                        followIndices.add(params.index());
                        persistentTasksService.sendRemoveRequest(
                                TaskIds.indexSyncTaskName(repository, indexName),
                                ActionListener.wrap(
                                        taskResponse -> logger.info("follower job deleted {} ", taskResponse),
                                        e -> logger.warn(" follower job deleted failed", e)
                                )
                        );
                    }
                }
            });

            Response response = followIndices.isEmpty() ?
                    new Response() :
                    new Response(followIndices.toArray(new String[followIndices.size()]));
            listener.onResponse(response);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }
    }

}
