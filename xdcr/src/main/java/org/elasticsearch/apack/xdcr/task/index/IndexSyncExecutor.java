package org.elasticsearch.apack.xdcr.task.index;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_BULK;

/**
 * IndexSyncExecutor 处理由 IndexSyncStartAction 注册的索引同步任务
 */
public class IndexSyncExecutor extends PersistentTasksExecutor<IndexSyncTaskParams> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    public IndexSyncExecutor(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(IndexSyncTaskParams.NAME, XDCR_THREAD_POOL_BULK);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    /**
     * 执行索引同步
     *
     * @param task   task
     * @param params IndexSyncTaskParams
     * @param state  PersistentTaskState
     */
    @Override
    protected void nodeOperation(AllocatedPersistentTask task, IndexSyncTaskParams params, PersistentTaskState state) {
        IndexSyncTask indexSyncTask = (IndexSyncTask) task;
        indexSyncTask.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<IndexSyncTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        IndexSyncTaskParams params = taskInProgress.getParams();
        return new IndexSyncTask(id, type, action, "xdcr-sync-index", parentTaskId, headers, params, clusterService, threadPool, client);
    }

}
