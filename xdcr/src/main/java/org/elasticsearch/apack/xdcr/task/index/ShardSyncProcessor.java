package org.elasticsearch.apack.xdcr.task.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.apack.xdcr.action.index.ShardSeqNoAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.apack.xdcr.action.index.ShardChangesAction;
import org.elasticsearch.apack.xdcr.action.index.bulk.BulkShardOperationsAction;
import org.elasticsearch.apack.xdcr.action.index.bulk.BulkShardOperationsRequest;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * shard 增量数据同步
 */
public class ShardSyncProcessor {

    private static final Logger logger = LogManager.getLogger(ShardSyncProcessor.class);

    private final Client client;
    private final Client remoteClient;
    private final ClusterService clusterService;
    private final String indexUUID;
    private final String index;
    private final int shard;

    private volatile boolean idle = false;
    private Consumer<Boolean> idleCallback;

    public ShardSyncProcessor(Client client, Client remoteClient, ClusterService clusterService, String indexUUID, String index, int shard, Consumer<Boolean> idleCallback) {
        this.client = client;
        this.remoteClient = remoteClient;
        this.clusterService = clusterService;
        this.indexUUID = indexUUID;
        this.index = index;
        this.shard = shard;
        this.idleCallback = idleCallback;
    }

    public void bulking() {
        try {
            doBulking();
        } catch (Exception e) {
            if (e.getMessage().contains("EsRejected") == false) {
                logger.error("shard bulking error", e);
            }
            idle(true);
        }
    }

    /**
     * 增量数据同步
     *
     * @throws Exception
     */
    private void doBulking() throws Exception {
        IndexRoutingTable indexRoutingTable = clusterService.state().routingTable().index(this.index);
        if (indexRoutingTable == null) {
            logger.trace("[shard-sync] index routing not found, process ignored: {} ", index);
            idle(true);
            return;
        }

        IndexShardRoutingTable shard = indexRoutingTable.shard(this.shard);
        if (shard == null) {
            logger.trace("[shard-sync] index shard not found, process ignored: [{}-{}]", this.index, this.shard);
            idle(true);
        }

        ShardRouting primaryShard = shard.primaryShard();
        if (primaryShard.active() == false) {
            logger.trace("[shard-sync] index primary shard not active, process ignored: {}", shard.getShardId());
            idle(true);
            return;
        }

        // 获取本集群shard maxSeqNo
        ShardSeqNoAction.Response response = client.execute(ShardSeqNoAction.INSTANCE, new ShardSeqNoAction.Request(this.index, this.shard)).get();
        long from = response.getSegNo();

        // 获取远程集群数据
        ShardChangesAction.Request request = new ShardChangesAction.Request(new ShardId(new Index(this.index, indexUUID), this.shard));
        request.setFromSeqNo(from + 1);
        ShardChangesAction.Response shardChangeResponse = remoteClient.execute(ShardChangesAction.INSTANCE, request).actionGet();

        // 写入到本集群
        if (shardChangeResponse.getOperations().length > 0) {
            logger.trace("[shard-sync] shard bulk start [seqno:{} , length:{}]", from, shardChangeResponse.getOperations().length);
            idle(false);
            List<Translog.Operation> operations = Arrays.asList(shardChangeResponse.getOperations());
            BulkShardOperationsRequest bulkRequest = new BulkShardOperationsRequest(
                    primaryShard.shardId(),
                    operations,
                    shardChangeResponse.getMaxSeqNoOfUpdatesOrDeletes());
            client.execute(BulkShardOperationsAction.INSTANCE, bulkRequest).actionGet();
        } else {
            logger.trace("[shard-sync] shard bulk ignored , cause no data found: {}", shard);
            idle(true);
        }
    }

    public void idle(boolean idle) {
        if (this.idle != idle) {
            idleCallback.accept(idle);
        }
        this.idle = idle;
    }

}
