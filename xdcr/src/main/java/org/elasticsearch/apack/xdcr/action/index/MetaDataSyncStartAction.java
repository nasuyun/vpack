/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.task.metadata.MetadataCoordinateTaskParams;
import org.elasticsearch.apack.xdcr.utils.ResponseHandler;

import java.io.IOException;

/**
 * 创建元数据同步任务
 * 内部任务
 * internal:* systemUser可执行
 */
public class MetaDataSyncStartAction extends Action<MetaDataSyncStartAction.Request, AcknowledgedResponse, MetaDataSyncStartAction.RequestBuilder> {

    public static final String TASK_ID = TaskIds.syncMetaTaskName();

    public static final MetaDataSyncStartAction INSTANCE = new MetaDataSyncStartAction();
    public static final String NAME = "internal:cluster/xdcr/syncmeta/start";


    private MetaDataSyncStartAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends MasterNodeRequest<Request> {


        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, MetaDataSyncStartAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            if (taskExisted(state)) {
                listener.onResponse(new AcknowledgedResponse(true));
                return;
            }
            final ResponseHandler handler = new ResponseHandler(listener);
            persistentTasksService.sendStartRequest(
                    TASK_ID, MetadataCoordinateTaskParams.NAME,
                    new MetadataCoordinateTaskParams(), handler.getActionListener());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }

        @Override
        protected String executor() {
            return XDCRSettings.XDCR_THREAD_POOL_MEATADATA_COORDINATE;
        }

        @Override
        protected AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }

        // 检查远程集群是否已存在
        private boolean taskExisted(ClusterState state) {
            PersistentTasksCustomMetaData persistentTasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
            return persistentTasks == null ? false :
                    persistentTasks.tasks().stream().anyMatch(task -> task.getTaskName().equals(MetadataCoordinateTaskParams.NAME));

        }
    }

}
