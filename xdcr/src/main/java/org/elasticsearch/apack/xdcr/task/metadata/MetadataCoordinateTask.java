package org.elasticsearch.apack.xdcr.task.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.threadpool.ThreadPool.Names.SAME;

/**
 * 元数据同步任务
 * <p>
 * 如果每个索引一个元数据同步会造成主集群master压力过大
 * 所以这里设计成一个集群只运行一个同步任务，负责所有索引的元数据同步过程。
 */
public class MetadataCoordinateTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(MetadataCoordinateTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final MetadataCoordinateTaskParams params;
    private final ThreadPool threadPool;
    private boolean cancelled = false;

    public MetadataCoordinateTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers,
                                  MetadataCoordinateTaskParams params,
                                  ClusterService clusterService,
                                  ThreadPool threadPool,
                                  Client client) {
        super(id, type, action, description, parentTask, headers);
        this.client = client;
        this.params = params;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * 周期性调度任务
     * ThreadPool.SAME 串行执行每次处理
     */
    public void start() {
        TimeValue interval = TimeValue.timeValueSeconds(10);
        new DriverTask(interval);
    }

    private class DriverTask extends AbstractAsyncTask {

        protected DriverTask(TimeValue interval) {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            try {
                new MetadataCoordinateProcessor(client, threadPool, clusterService).perform();
            } catch (Exception e) {
                logger.error("[metadata-sync-driver] process error found: {}", e);
            }
        }

        @Override
        protected boolean mustReschedule() {
            return cancelled == false;
        }

        @Override
        protected String getThreadPool() {
            return SAME;
        }
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
        cancelled = true;
    }

}
