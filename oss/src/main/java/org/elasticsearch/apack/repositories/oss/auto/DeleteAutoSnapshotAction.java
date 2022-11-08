/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.repositories.oss.auto;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.apack.repositories.oss.auto.AutoSnapshotTask.AUTO_SNAPSHOT_TASK_PREFIX;
import static org.elasticsearch.rest.RestRequest.Method.*;

public class DeleteAutoSnapshotAction {

    public static class Action extends org.elasticsearch.action.Action<Request, Response, RequestBuilder> {

        public static final Action INSTANCE = new Action();
        public static final String NAME = "indices:admin/apack/repository/oss/delete_auto_snapshot";

        protected Action() {
            super(NAME);
        }

        @Override
        public Response newResponse() {
            return new Response();
        }

        @Override
        public RequestBuilder newRequestBuilder(final ElasticsearchClient client) {
            return new RequestBuilder(client, INSTANCE);
        }
    }

    /**
     * Response
     */
    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response() {
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {

        private String repository;

        public Request() {
        }

        public Request(String repository) {
            this.repository = repository;
        }

        public String getTaskId() {
            return AUTO_SNAPSHOT_TASK_PREFIX + repository;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.repository = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
        }

    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(
                final ElasticsearchClient client,
                final org.elasticsearch.action.Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(
                final Settings settings, final ThreadPool threadPool, final TransportService transportService,
                final ClusterService clusterService, final ActionFilters actionFilters, final IndexNameExpressionResolver indexNameExpressionResolver,
                final PersistentTasksService persistentTasksService

        ) {
            super(
                    settings,
                    Action.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    indexNameExpressionResolver,
                    Request::new);

            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of streamable is to be replaced by Writeable");
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in.readBoolean());
        }

        @Override
        protected void masterOperation(
                final Request request,
                final ClusterState state,
                final ActionListener<Response> listener) {

            final ResponseHandler handler = new ResponseHandler(listener);
            if (hasTask(request.getTaskId(), state)) {
                persistentTasksService.sendRemoveRequest(request.getTaskId(), handler.getActionListener());
            } else {
                listener.onResponse(new Response(false));
            }
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.toString());
        }
    }

    public static boolean hasTask(String taskId, ClusterState clusterState) {
        PersistentTasksCustomMetaData.Builder builder = PersistentTasksCustomMetaData.builder(clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
        return builder.hasTask(taskId);
    }

    // =======================================================
    //                        REST MODULE
    // =======================================================

    public static class RestDeleteAutoSnapshot extends BaseRestHandler {

        public RestDeleteAutoSnapshot(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(DELETE, "/_snapshot/{repository}/_auto", this);
        }

        @Override
        public String getName() {
            return "apack_delete_snapshot_auto";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
            String repository = restRequest.param("repository");
            Request request = new Request(repository);
            request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
            request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
            return channel -> client.execute(Action.INSTANCE, request, new RestToXContentListener<>(channel));
        }
    }

}
