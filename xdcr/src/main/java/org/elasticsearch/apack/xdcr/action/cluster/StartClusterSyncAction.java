/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.action.cluster;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.apack.xdcr.task.cluster.ClusterLevelCoordinateTaskParams;
import org.elasticsearch.apack.xdcr.utils.RemoteConnections;
import org.elasticsearch.apack.xdcr.utils.ResponseHandler;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.apack.xdcr.utils.Actions.always;

/**
 * 创建集群级同步任务
 */
public class StartClusterSyncAction extends Action<ClusterSyncRequest, AcknowledgedResponse, ClusterSyncRequest.Builder> {

    public static final StartClusterSyncAction INSTANCE = new StartClusterSyncAction();
    public static final String NAME = "cluster:admin/xdcr/cluster/sync/start";

    private StartClusterSyncAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    public ClusterSyncRequest.Builder newRequestBuilder(ElasticsearchClient client) {
        return new ClusterSyncRequest.Builder(client, INSTANCE, new ClusterSyncRequest());
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<ClusterSyncRequest, AcknowledgedResponse> {

        private final PersistentTasksService persistentTasksService;
        private final Client client;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, StartClusterSyncAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, ClusterSyncRequest::new);
            this.persistentTasksService = persistentTasksService;
            this.client = client;
        }

        @Override
        protected void masterOperation(ClusterSyncRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {

            if (RemoteConnections.isConnected(client, request.repository()) == false) {
                throw new RemoteTransportException("error while communicating with remote cluster [" + request.repository() + "]", null);
            }

            // restart task
            final ResponseHandler handler = new ResponseHandler(listener);
            ClusterLevelCoordinateTaskParams taskParams =
                    new ClusterLevelCoordinateTaskParams(
                            request.repository(),
                            request.inclouds(),
                            request.excludes(),
                            threadPool.getThreadContext().getHeaders());
            if (hasTask(request.getTaskId(), state)) {
                persistentTasksService.sendRemoveRequest(request.getTaskId(), always(
                        r -> persistentTasksService.sendStartRequest(
                                request.getTaskId(), ClusterLevelCoordinateTaskParams.NAME,
                                taskParams, handler.getActionListener())));
            } else {
                persistentTasksService.sendStartRequest(
                        request.getTaskId(), ClusterLevelCoordinateTaskParams.NAME,
                        taskParams, handler.getActionListener());
            }
        }

        @Override
        protected ClusterBlockException checkBlock(ClusterSyncRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.repository());
        }

        @Override
        protected String executor() {
            return XDCRSettings.XDCR_THREAD_POOL_CLUSTER_COORDINATE;
        }

        @Override
        protected AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }

    }

    public static boolean hasTask(String taskId, ClusterState clusterState) {
        PersistentTasksCustomMetaData.Builder builder = PersistentTasksCustomMetaData.builder(clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
        return builder.hasTask(taskId);
    }

}
