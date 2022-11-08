package org.elasticsearch.apack.repositories.oss.auto;

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

public class AutoSnapshotTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(AutoSnapshotTask.class);
    public static final String AUTO_SNAPSHOT_TASK_PREFIX = "auto-snapshot-";
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    private final String repository;
    private final TimeValue interval;
    private final int retain;
    private final String[] indices;

    private boolean cancelled = false;
    private DriverTask driverTask;

    public AutoSnapshotTask(long id, String type, String action, String description,
                            TaskId parentTask, Map<String, String> headers, AutoSnapshotTaskParams params,
                            ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(id, type, action, description, parentTask, headers);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repository = params.repository();
        this.interval = params.interval() != null ? params.interval() : TimeValue.timeValueHours(2);
        this.retain = params.retain();
        this.indices = params.indices();
    }

    public void start() {
        driverTask = new DriverTask(interval);
    }

    private class DriverTask extends AbstractAsyncTask {

        protected DriverTask(TimeValue interval) {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            new AutoSnapshotProcessor(client, clusterService, threadPool, repository, retain, indices).process();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected boolean mustReschedule() {
            return cancelled == false;
        }
    }

    /**
     * 当外部取消任务
     */
    @Override
    protected void onCancelled() {
        markAsCompleted();
        this.cancelled = true;
    }

}
