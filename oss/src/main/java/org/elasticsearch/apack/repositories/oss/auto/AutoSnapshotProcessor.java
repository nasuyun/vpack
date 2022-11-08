package org.elasticsearch.apack.repositories.oss.auto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class AutoSnapshotProcessor {

    private static final Logger logger = LogManager.getLogger(AutoSnapshotProcessor.class);
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(60);

    private final Client client;
    private final String repository;
    private final int retain;
    private final String[] indices;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    AutoSnapshotProcessor(Client client, ClusterService clusterService, ThreadPool threadPool,
                          String repository, int retain, String[] indices) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repository = repository;
        this.retain = retain;
        this.indices = indices;
    }

    public void process() {
        createSnapshotAndCleanExpire();
    }

    private void createSnapshotAndCleanExpire() {
        try {
            waitForSnapshotCompletion("auto-create-snapshot", DEFAULT_TIMEOUT, () -> {
                String snapshot = dateFormater.format(new Date());
                CreateSnapshotRequest request = new CreateSnapshotRequest();
                request.repository(repository);
                request.snapshot(snapshot);
                if (indices != null && indices.length > 0) {
                    request.indices(indices);
                }
                logger.info("[auto-snapshot] create snapshot [{}] [{}]", repository, snapshot);
                client.admin().cluster().createSnapshot(request, ActionListener.wrap(
                        r -> cleanExpire(),
                        e -> logger.error("[auto-snapshot] create snapshot failure", e)
                ));
            });
        } catch (Exception e) {
            logger.error("[auto-snapshot] create snapshot failure", e);
        }
    }

    private void cleanExpire() {
        client.admin().cluster().prepareGetSnapshots(repository).addSnapshots("*").execute(
                ActionListener.wrap(
                        r -> {
                            List<SnapshotInfo> snapshots = r.getSnapshots();
                            if (snapshots == null || snapshots.isEmpty()) {
                                return;
                            }
                            if (snapshots.size() <= retain) {
                                return;
                            }
                            for (int i = 0; i < snapshots.size() - retain; i++) {
                                deleteSnapshot(snapshots.get(i).snapshotId().getName());
                            }
                        },
                        e -> logger.warn("[auto-snapshot] get snapshot failure", e)
                ));
    }

    private void deleteSnapshot(String snapshot) {
        threadPool.generic().submit(() -> {
            try {
                waitForSnapshotCompletion("delete-snapshot", DEFAULT_TIMEOUT, () -> {
                    DeleteSnapshotRequest request = new DeleteSnapshotRequest();
                    request.repository(repository);
                    request.snapshot(snapshot);
                    logger.info("[auto-snapshot] auto delete snapshot [{}] [{}]", repository, snapshot);
                    client.admin().cluster().deleteSnapshot(request).actionGet(DEFAULT_TIMEOUT);
                });
            } catch (Exception e) {
                logger.warn("[auto-snapshot] auto delete snapshot [{}] [{}] failure ", repository, snapshot, e);
            }
        });

    }

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(1000);

    private volatile Lock lock = new ReentrantLock();

    public void waitForSnapshotCompletion(String taskName, TimeValue timeout, CheckedRunnable checkedRunnable) throws Exception {
        long offset = System.currentTimeMillis() + timeout.millis();
        while (offset - System.currentTimeMillis() > 0) {
            ClusterState state = clusterService.state();
            SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            if (isCompleted(snapshotsInProgress) && isCompleted(deletionsInProgress)) {
                lock.lock();
                try {
                    checkedRunnable.run();
                } finally {
                    lock.unlock();
                }
                return;
            }
            Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
        }
        logger.warn("[auto-snapshot] task[{}] cancelled, because wait for snapshot all completion time out", taskName);
    }

    private static boolean isCompleted(SnapshotsInProgress snapshotsInProgress) {
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return true;
        }
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.state().completed() == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean isCompleted(SnapshotDeletionsInProgress snapshotsInProgress) {
        return snapshotsInProgress == null || snapshotsInProgress.getEntries().isEmpty();
    }

    private static SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");

}
