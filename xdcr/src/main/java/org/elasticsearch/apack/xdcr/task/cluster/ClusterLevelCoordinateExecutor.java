package org.elasticsearch.apack.xdcr.task.cluster;

import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_CLUSTER_COORDINATE;

public class ClusterLevelCoordinateExecutor extends PersistentTasksExecutor<ClusterLevelCoordinateTaskParams> {

    private final XDCRSettings xdcrSettings;
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public ClusterLevelCoordinateExecutor(XDCRSettings xdcrSettings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(ClusterLevelCoordinateTaskParams.NAME, XDCR_THREAD_POOL_CLUSTER_COORDINATE);
        this.xdcrSettings = xdcrSettings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ClusterLevelCoordinateTaskParams params, PersistentTaskState state) {
        ClusterLevelCoordinateTask coordinateTask = (ClusterLevelCoordinateTask) task;
        coordinateTask.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<ClusterLevelCoordinateTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        ClusterLevelCoordinateTaskParams params = taskInProgress.getParams();
        return new ClusterLevelCoordinateTask(id, type, action, "xdcr-sync-cluster", parentTaskId, headers, params,
                xdcrSettings,
                clusterService,
                threadPool,
                client);
    }

}
