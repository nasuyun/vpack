/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.repositories.oss.auto;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.apack.repositories.oss.auto.AutoSnapshotTask.AUTO_SNAPSHOT_TASK_PREFIX;
import static org.elasticsearch.apack.repositories.oss.auto.ResponseHandler.always;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class CreateAutoSnapshotAction {

    public static class Action extends org.elasticsearch.action.Action<Request, Response, RequestBuilder> {

        public static final Action INSTANCE = new Action();
        public static final String NAME = "indices:admin/apack/repository/oss/create_auto_snapshot";

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
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {

        /**
         * Repository
         */
        private String repository;
        /**
         * 自动备份间隔时间
         */
        private TimeValue interval;
        /**
         * 保留版本数
         */
        private Integer retain;
        /**
         * 备份索引
         */
        private String[] indices;
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        public Request() {
        }

        public Request(String repository) {
            this.repository = repository;
        }

        public Request interval(TimeValue interval) {
            this.interval = interval;
            return this;
        }

        public Request retain(Integer retain) {
            this.retain = retain;
            return this;
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        public Request indices(List<String> indices) {
            this.indices = indices.toArray(new String[indices.size()]);
            return this;
        }

        public Request indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public Request source(Map<String, Object> source) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                String name = entry.getKey();
                if (name.equals("indices")) {
                    if (entry.getValue() instanceof String) {
                        indices(Strings.splitStringByCommaToArray((String) entry.getValue()));
                    } else if (entry.getValue() instanceof ArrayList) {
                        indices((ArrayList<String>) entry.getValue());
                    } else {
                        throw new IllegalArgumentException("malformed indices section, should be an array of strings");
                    }
                } else if (name.equals("interval")) {
                    interval(TimeValue.parseTimeValue((String) entry.getValue(), ""));
                } else if (name.equals("retain")) {
                    retain((Integer) entry.getValue());
                }
            }
            indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
            return this;
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
            this.interval = TimeValue.timeValueMillis(in.readLong());
            this.retain = in.readInt();
            int size = in.readInt();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            if (size > 0) {
                this.indices = in.readStringArray();
            }
            this.indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeLong(interval.millis());
            out.writeInt(retain);
            indicesOptions.writeIndicesOptions(out);
            if (indices == null || indices.length == 0) {
                out.writeInt(0);
            } else {
                out.writeInt(indices.length);
                out.writeStringArray(indices);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.deepEquals(indices, other.indices)
                    && Objects.equals(indicesOptions, other.indicesOptions);
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

            RepositoriesMetaData repositories = state.metaData().custom(RepositoriesMetaData.TYPE);
            String repository = request.repository;
            if (repositories.repository(repository) == null) {
                listener.onFailure(new RepositoryMissingException(repository));
                return;
            }

            final ResponseHandler handler = new ResponseHandler(listener);
            AutoSnapshotTaskParams params = new AutoSnapshotTaskParams(repository, request.interval, request.retain, request.indices);
            if (hasTask(request.getTaskId(), state)) {
                persistentTasksService.sendRemoveRequest(request.getTaskId(), always(
                        r -> persistentTasksService.sendStartRequest(
                                request.getTaskId(), AutoSnapshotTaskParams.NAME,
                                params, handler.getActionListener())));
            } else {
                persistentTasksService.sendStartRequest(
                        request.getTaskId(), AutoSnapshotTaskParams.NAME,
                        params, handler.getActionListener());
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

    // ======================================
    //              REST Action
    // ======================================

    public static class RestCreateAutoSnapshot extends BaseRestHandler {

        public RestCreateAutoSnapshot(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(PUT, "/_snapshot/{repository}/_auto", this);
            controller.registerHandler(POST, "/_snapshot/{repository}/_auto", this);
        }

        @Override
        public String getName() {
            return "apack_create_snapshot_auto";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            Request request = new Request(restRequest.param("repository"));
            request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
            request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
            request.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
            restRequest.applyContentParser(p -> request.source(p.mapOrdered()));
            return channel -> client.execute(Action.INSTANCE, request, new RestToXContentListener<>(channel));
        }

    }

}
