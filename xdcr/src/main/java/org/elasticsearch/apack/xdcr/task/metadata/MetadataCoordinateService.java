package org.elasticsearch.apack.xdcr.task.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.apack.xdcr.action.index.MetaDataSyncStartAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.apack.xdcr.utils.Ssl.systemClient;

public class MetadataCoordinateService {

    private static final Logger logger = LogManager.getLogger(MetadataCoordinateService.class);

    private final Client client;
    private final ThreadPool threadPool;

    /**
     * 元数据同步，单线程驱动执行
     *
     * @param client     Client
     * @param threadPool ThreadPool
     */
    public MetadataCoordinateService(Client client, ThreadPool threadPool) {
        this.client = systemClient(client);
        this.threadPool = threadPool;
        new DrivingMetaUpdatePersistentTask();
    }

    private class DrivingMetaUpdatePersistentTask extends AbstractAsyncTask {

        private boolean taskStarted = false;

        public DrivingMetaUpdatePersistentTask() {
            super(logger, threadPool, TimeValue.timeValueSeconds(5), true);
            rescheduleIfNecessary();
        }

        @Override
        protected void runInternal() {
            client.execute(MetaDataSyncStartAction.INSTANCE, new MetaDataSyncStartAction.Request(), new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    taskStarted = acknowledgedResponse.isAcknowledged();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(e);
                }
            });
        }

        @Override
        protected boolean mustReschedule() {
            return taskStarted == false;
        }

    }


}
