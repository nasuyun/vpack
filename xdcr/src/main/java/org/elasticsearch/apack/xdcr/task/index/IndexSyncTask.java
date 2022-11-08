package org.elasticsearch.apack.xdcr.task.index;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Map;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_BULK;
import static org.elasticsearch.apack.xdcr.utils.Ssl.*;

public class IndexSyncTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(IndexSyncTask.class);
    // 如果远程集群seqno不相等 则置立即执行
    private static final TimeValue BULKING_WAIT_IMMEDIATELY = TimeValue.timeValueMillis(100);
    // 如果远程集群seqno相等  则置为闲置状态 下次执行等待1分钟
    private static final TimeValue BULKING_WAIT_IDEL = TimeValue.timeValueSeconds(60);

    private final Client client;
    private final Client remoteClient;
    private final ClusterService clusterService;
    private final IndexSyncTaskParams params;
    private final ThreadPool threadPool;
    private final String index;

    // shard同步任务
    private final List<ShardReplicationTask> shardReplicationTaskHolder;
    private volatile boolean isShardReplicaRunning = false;
    private volatile boolean isTaskCancelled = false;

    public IndexSyncTask(long id, String type, String action, String description,
                         TaskId parentTask, Map<String, String> headers, IndexSyncTaskParams params,
                         ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.client = userClient(client, params.headers());
        this.remoteClient = userClient(remoteClient(client, params.repository()), params.headers());
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.index = params.index();
        this.shardReplicationTaskHolder = new ArrayList();
    }

    public IndexSyncTaskParams params() {
        return params;
    }

    public void start() {
        new IndexReplicationDriver();
    }

    /**
     * 驱动每个shard开始同步
     */
    private class IndexReplicationDriver extends AbstractAsyncTask {

        protected IndexReplicationDriver() {
            super(logger, threadPool, TimeValue.timeValueSeconds(5), true);
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            MetaData metaData = clusterService.state().metaData();
            IndexMetaData indexMetaData = metaData.index(index);

            if (metaData.hasIndex(index)) {
                if (isShardReplicaRunning == false) {
                    startShardReplicating(indexMetaData);
                }
                isShardReplicaRunning = true;
            } else {
                stopAndCleanShardReplicating();
                isShardReplicaRunning = false;
            }
        }

        @Override
        protected boolean mustReschedule() {
            return isTaskCancelled == false;
        }
    }

    /**
     * 启动所有shard同步任务
     */
    private void startShardReplicating(IndexMetaData indexMetaData) {
        String indexUUID = indexMetaData
                .getCustomData(XDCRSettings.XDCR_CUSTOM_METADATA_KEY)
                .get(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING.getKey());
        int shardNums = indexMetaData.getNumberOfShards();
        for (int i = 0; i < shardNums; i++) {
            shardReplicationTaskHolder.add(new ShardReplicationTask(indexUUID, i));
        }
    }

    /**
     * 清除所有shard同步任务
     */
    private void stopAndCleanShardReplicating() {
        shardReplicationTaskHolder.forEach(task -> task.close());
        shardReplicationTaskHolder.clear();
    }

    /**
     * shard 同步任务
     */
    private class ShardReplicationTask extends AbstractAsyncTask {

        final ShardSyncProcessor shardSyncProcessor;

        protected ShardReplicationTask(String indexUUID, int shard) {
            super(logger, threadPool, TimeValue.timeValueSeconds(1), true);
            this.shardSyncProcessor = new ShardSyncProcessor(
                    client, remoteClient, clusterService, indexUUID, index, shard,
                    idle -> this.setInterval(idle ? BULKING_WAIT_IDEL : BULKING_WAIT_IMMEDIATELY));
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            shardSyncProcessor.bulking();
        }

        @Override
        protected String getThreadPool() {
            return XDCR_THREAD_POOL_BULK;
        }

        @Override
        protected boolean mustReschedule() {
            return isTaskCancelled == false;
        }
    }

    /**
     * 当外部取消任务
     */
    @Override
    protected void onCancelled() {
        markAsCompleted();
        this.isTaskCancelled = true;
    }

}
