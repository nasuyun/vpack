package org.elasticsearch.apack.xdcr.task.metadata;

import org.elasticsearch.apack.xdcr.task.TaskIds;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_MEATADATA_COORDINATE;

/**
 * 元数据同步后台任务
 * 单线程执行
 */
public class MetadataCoordinateExecutor extends PersistentTasksExecutor<MetadataCoordinateTaskParams> {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;

    /**
     * 元数据同步后台任务
     *
     * @param clusterService ClusterService
     * @param threadPool     ThreadPool
     * @param client         Client
     */
    public MetadataCoordinateExecutor(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(MetadataCoordinateTaskParams.NAME, XDCR_THREAD_POOL_MEATADATA_COORDINATE);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    /**
     * 元数据同步执行过程
     *
     * @param task   AllocatedPersistentTask
     * @param params MetadataSyncTaskParams
     * @param state  PersistentTaskState
     */
    @Override
    protected void nodeOperation(AllocatedPersistentTask task, MetadataCoordinateTaskParams params, PersistentTaskState state) {
        MetadataCoordinateTask metaDataCoordinateTask = (MetadataCoordinateTask) task;
        metaDataCoordinateTask.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<MetadataCoordinateTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        MetadataCoordinateTaskParams params = taskInProgress.getParams();
        return new MetadataCoordinateTask(id, type, action, TaskIds.SYNC_METADATA_TASKNAME, parentTaskId, headers,
                params,
                clusterService,
                threadPool,
                client);
    }
}
