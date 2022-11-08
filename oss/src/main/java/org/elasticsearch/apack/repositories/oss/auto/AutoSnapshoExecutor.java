package org.elasticsearch.apack.repositories.oss.auto;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class AutoSnapshoExecutor extends PersistentTasksExecutor<AutoSnapshotTaskParams> {

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public AutoSnapshoExecutor(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(AutoSnapshotTaskParams.NAME, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, AutoSnapshotTaskParams params, PersistentTaskState state) {
        AutoSnapshotTask auto = (AutoSnapshotTask) task;
        auto.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<AutoSnapshotTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        AutoSnapshotTaskParams params = taskInProgress.getParams();
        return new AutoSnapshotTask(id, type, action, "auto-snapshot", parentTaskId, headers, params,
                clusterService,
                threadPool,
                client);
    }

}
