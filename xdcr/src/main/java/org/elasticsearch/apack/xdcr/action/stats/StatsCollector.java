package org.elasticsearch.apack.xdcr.action.stats;

import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.apack.xdcr.task.TaskUtil;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.seqno.SeqNoStats;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Collector for XDCRStats
 */
public class StatsCollector {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(10);
    private final Client client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public StatsCollector(Client client, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public StatsAction.Response collect(StatsAction.Request request, ClusterState state) {
        Map<String, List<String>> syncMap = TaskUtil.syncMap(state);
        String requestRepository = request.repository();
        String requestIndex = request.index();
        if (Strings.isEmpty(requestRepository) == false) {
            if (syncMap.containsKey(requestRepository)) {
                Map<String, List<String>> filtedSyncMap = new LinkedHashMap<>();
                filtedSyncMap.put(requestRepository, syncMap.get(requestRepository));
                syncMap = filtedSyncMap;
            } else {
                return new StatsAction.Response(Collections.emptyList());
            }
        }

        if (Strings.isEmpty(requestIndex) == false) {
            Map<String, List<String>> filtedSyncMap = new LinkedHashMap<>();
            Set<String> concreteIndices = indexNameExpressionResolver.resolveExpressions(state, requestIndex);
            syncMap.forEach((k, v) -> {
                List<String> indices = v.stream().filter(idx -> concreteIndices.contains(idx)).collect(Collectors.toList());
                filtedSyncMap.put(k, indices);
            });
            syncMap = filtedSyncMap;
        }

        List<Stats> result = new ArrayList<>();
        syncMap.forEach((repository, indices) -> {
            Map<String, IndexStats> localStats = stats(client, indices.stream().toArray(String[]::new));
            Map<String, IndexStats> remoteStats = stats(client.getRemoteClusterClient(repository), indices.stream().toArray(String[]::new));

            for (String index : indices) {
                Map<Integer, SeqNoStats> local = generate(localStats, index);
                Map<Integer, SeqNoStats> remote = generate(remoteStats, index);

                Map<Integer, Stats> merge = new LinkedHashMap();
                local.forEach((shard, shardStats) -> {
                    merge.put(shard, new Stats(repository, index, shard, shardStats, emptySeqNoStats));
                });
                remote.forEach((shard, shardStats) -> {
                    if (merge.containsKey(shard)) {
                        Stats stats = merge.get(shard);
                        stats.remoteSeqNoStats = shardStats;
                    } else {
                        merge.put(shard, new Stats(repository, index, shard, emptySeqNoStats, shardStats));
                    }
                });
                if (merge.isEmpty()) {
                    result.add(new Stats(repository, index, -1, emptySeqNoStats, emptySeqNoStats));
                } else {
                    result.addAll(merge.values());
                }
            }
        });
        return new StatsAction.Response(result);
    }

    private static Map<String, IndexStats> stats(Client client, String... indices) {
        try {
            IndicesStatsResponse statsResponse = client.admin().indices().prepareStats(indices).clear().setDocs(true).get(TIMEOUT);
            return statsResponse.getIndices();
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static Map<Integer, SeqNoStats> generate(Map<String, IndexStats> stats, String index) {
        Map<Integer, SeqNoStats> result = new LinkedHashMap<>();
        if (stats.containsKey(index) == false) {
            return Collections.emptyMap();
        }
        IndexStats indexStats = stats.get(index);
        for (IndexShardStats indexStat : indexStats) {
            for (ShardStats shard : indexStat.getShards()) {
                result.put(shard.getShardRouting().id(), shard.getSeqNoStats());
            }
        }
        return result;
    }

    private static SeqNoStats emptySeqNoStats = new SeqNoStats(-1, -1, -1);


}