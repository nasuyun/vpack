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
import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.apack.xdcr.task.TaskUtil;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.utils.ResponseHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.apack.xdcr.task.TaskIds.repositoryIndex;

/**
 * 删除集群级同步任务
 * 同时删除所有手动触发的同步任务
 */
public class StopClusterSyncAction extends Action<ClusterSyncRequest, AcknowledgedResponse, ClusterSyncRequest.Builder> {

    public static final StopClusterSyncAction INSTANCE = new StopClusterSyncAction();
    public static final String NAME = "cluster:admin/xdcr/cluster/sync/stop";

    private StopClusterSyncAction() {
        super(NAME);
    }

    @Override
    public ClusterSyncRequest.Builder newRequestBuilder(ElasticsearchClient client) {
        return new ClusterSyncRequest.Builder(client, INSTANCE, new ClusterSyncRequest());
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<ClusterSyncRequest, AcknowledgedResponse> {

        private final Settings settings;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, StopClusterSyncAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, ClusterSyncRequest::new);
            this.settings = settings;
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void masterOperation(ClusterSyncRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            final ResponseHandler responseHandler = new ResponseHandler(listener);
            // 删除索引同步任务
            Map<String, PersistentTasksCustomMetaData.PersistentTask<?>> taskMap = PersistentTasksCustomMetaData.builder(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE)).build().taskMap();
            taskMap.forEach((k, v) -> {
                if (k.startsWith(TaskIds.SYNC_CLUSTER_TASKNAME_PREFIX)) {
                    persistentTasksService.sendRemoveRequest(k, responseHandler.getActionListener());
                }
                if (k.startsWith(TaskIds.SYNC_INDEX_TASKNAME_PREFIX)) {
                    persistentTasksService.sendRemoveRequest(k, responseHandler.getActionListener());
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(ClusterSyncRequest request, ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.repository());
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }

    }
}
