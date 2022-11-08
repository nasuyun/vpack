package org.elasticsearch.apack.xdcr.task.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_CLUSTER_COORDINATE;

public class ClusterLevelCoordinateTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(ClusterLevelCoordinateTask.class);

    private final XDCRSettings xdcrSettings;
    private final Client client;
    private final ClusterService clusterService;
    private final ClusterLevelCoordinateTaskParams params;
    private final ThreadPool threadPool;
    private boolean cancelled = false;
    private DriverTask driverTask;

    public ClusterLevelCoordinateTask(long id, String type, String action, String description,
                                      TaskId parentTask, Map<String, String> headers, ClusterLevelCoordinateTaskParams params,
                                      XDCRSettings xdcrSettings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.client = client;
        this.xdcrSettings = xdcrSettings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.xdcrSettings.setClusterLevelCoordinateIntervalConsumer(this::updateInterval);
    }

    public void start() {
        driverTask = new DriverTask(xdcrSettings.getClusterLevelCoordinateInterval());
    }

    private void updateInterval(TimeValue interval) {
        if (driverTask != null) {
            driverTask.setInterval(interval);
        }
    }

    private class DriverTask extends AbstractAsyncTask {

        protected DriverTask(TimeValue interval) {
            super(logger, threadPool, interval, true);
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            new ClusterLevelCoordinateProcessor(clusterService, client, params).process();
        }

        protected String getThreadPool() {
            return XDCR_THREAD_POOL_CLUSTER_COORDINATE;
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
