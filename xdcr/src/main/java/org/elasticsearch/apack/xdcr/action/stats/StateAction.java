/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.action.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StateAction extends Action<StateAction.Request, StateAction.Response, StateAction.Builder> {

    public static final StateAction INSTANCE = new StateAction();
    public static final String NAME = "cluster:monitor/xdcr/state";

    private StateAction() {
        super(NAME);
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
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

    public static class Response extends ActionResponse implements ToXContentObject {

        Map<String, PersistentTask<?>> tasks;

        public Response(Map<String, PersistentTask<?>> tasks) {
            this.tasks = tasks;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            tasks = in.readMap(StreamInput::readString, PersistentTask::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            Map<String, PersistentTask<?>> filteredTasks = tasks.values().stream()
                    .filter(t -> ClusterState.FeatureAware.shouldSerialize(out, t.getParams()))
                    .collect(Collectors.toMap(PersistentTask::getId, Function.identity()));
            out.writeMap(filteredTasks, StreamOutput::writeString, (stream, value) -> value.writeTo(stream));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("tasks");
            for (PersistentTask<?> entry : tasks.values()) {
                builder.startObject();
                builder.field(entry.getId(), entry.getParams());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        @Inject
        public Transport(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ClusterService clusterService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final Client client) {
            super(
                    settings,
                    StateAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    Request::new,
                    indexNameExpressionResolver);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of streamable is to be replaced by Writeable");
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(
                final Request request,
                final ClusterState state,
                final ActionListener<Response> listener) {
            try {
                Map<String, PersistentTask<?>> taskMap = PersistentTasksCustomMetaData.builder(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE)).build().taskMap();
                Map<String, PersistentTask<?>> filteredMap = taskMap.entrySet()
                        .stream()
                        .filter(x -> x.getKey().startsWith(TaskIds.SYNC_CLUSTER_TASKNAME_PREFIX) || x.getKey().startsWith(TaskIds.SYNC_INDEX_TASKNAME_PREFIX))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                listener.onResponse(new Response(filteredMap));
            } catch (Exception e) {
                listener.onFailure(new ElasticsearchException(e));
            }
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.toString());
        }
    }

}
