package org.elasticsearch.apack.xdcr.task.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.apack.xdcr.action.index.CreateFollowerAction;
import org.elasticsearch.apack.xdcr.action.index.DeleteFollowJobAction;
import org.elasticsearch.apack.xdcr.task.index.IndexSyncTaskParams;
import org.elasticsearch.apack.xdcr.utils.Actions;
import org.elasticsearch.apack.xdcr.utils.SettingUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.apack.xdcr.utils.Actions.always;
import static org.elasticsearch.apack.xdcr.utils.Actions.success;
import static org.elasticsearch.apack.xdcr.utils.Ssl.remoteClient;
import static org.elasticsearch.apack.xdcr.utils.Ssl.userClient;
import static org.elasticsearch.common.settings.IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

/**
 * 元数据同步器:检查PersistentTask内的索引同步任务，处理元数据同步
 * 集群级任务，避免每个索引执行自己的元数据同步，消耗过多资源。
 * 元数据同步过程：
 * - 未发现索引则创建
 * - 存在则检查mapping settings等元数据进行同步
 */
public class MetadataCoordinateProcessor {

    private static final Logger logger = LogManager.getLogger(MetadataCoordinateTask.class);
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(60);
    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private Map<String, List<IndexSyncTaskParams>> repositoryGroupedTasks;

    public MetadataCoordinateProcessor(Client client, ThreadPool threadPool, ClusterService clusterService) {
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    public void perform() {
        logger.debug("[metadata-sync] process started");
        repositoryGroupedTasks = new HashMap<>();
        PersistentTasksCustomMetaData persistentTasksCustomMetaData = clusterService.state().metaData().custom(PersistentTasksCustomMetaData.TYPE);
        for (PersistentTask ptask : persistentTasksCustomMetaData.tasks()) {
            if (ptask.getTaskName().equals(IndexSyncTaskParams.NAME)) {
                IndexSyncTaskParams params = (IndexSyncTaskParams) ptask.getParams();
                repositoryGroupedTasks.computeIfAbsent(params.repository(), k -> new ArrayList()).add(params);
            }
        }
        repositoryGroupedTasks.forEach((k, v) -> performInternal(k, v));
    }

    private void performInternal(String repository, List<IndexSyncTaskParams> params) {
        Client localClient = userClient(client, params.get(0).headers());
        Client remoteClient = userClient(remoteClient(client, repository), params.get(0).headers());
        List<String> indices = params.stream().map(f -> f.index()).collect(Collectors.toList());
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(indices.toArray(new String[indices.size()]));
        remoteClient.admin().cluster().state(clusterStateRequest,
                ActionListener.wrap(
                        remoteClusterStateResponse ->
                                syncRepository(
                                        repository,
                                        params,
                                        localClient,
                                        clusterService.state(),
                                        remoteClient,
                                        remoteClusterStateResponse.getState()
                                ),
                        failure -> logger.error("[metadata-sync] get remote cluster state error {} ", failure)
                )
        );
    }

    private void syncRepository(String repository,
                                List<IndexSyncTaskParams> indicesParams,
                                Client localClient, ClusterState localState,
                                Client remoteClient, ClusterState remoteState) {

        indicesParams.forEach(indexParams -> {

            // ============================
            // ======== 索引同步过程 ========
            // ============================
            MetaData localMetaData = localState.metaData();
            MetaData remoteMetaData = remoteState.metaData();

            logger.trace("[metadata-sync] metadata coordinate for index {}", indexParams.index());
            String index = indexParams.index();
            IndexMetaData leaderIndexMetaData = remoteMetaData.index(index);
            // 如果远程未发现索引
            if (remoteMetaData.hasIndex(index) == false) {
                logger.debug("[metadata-sync] leader index not found {}", index);
                return;
            }

            // 不存在则忽略
            if (leaderIndexMetaData == null) {
                logger.info("[metadata-sync] process skipped, leader index not found {}", index);
                return;
            }

            // 检查 leader-index softdelete，目前不做处理
            if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetaData.getSettings()) == false) {
                logger.info("[metadata-sync] process skipped, leader index soft_delete disabled {}", index);
                return;
            }

            // 本地不存在则创建
            // 创建过程前检查leader-index所有主分片是否处于START服务状态
            // 未START状态下snapshot恢复过程会出错
            IndexMetaData localIndexMetaData = localMetaData.index(index);
            if (localMetaData.hasIndex(index) == false) {
                if (localIndexMetaData == null) {
                    int shards = leaderIndexMetaData.getNumberOfShards();
                    if (leaderIndexAllShardsStarted(remoteClient, index, shards) == false) {
                        logger.info("[metadata-sync] process skipped, wait leader index shard all active [{}] shards[{}]", index, shards);
                        return;
                    }

                    try {
                        MetaDataCreateIndexService.validateIndexName(index, localState);
                    } catch (Exception e) {
                        // index alrady exited
                        return;
                    }

                    remoteClient.admin().indices().prepareFlush(index).execute(
                            ListenerTimeouts.wrapWithTimeout(
                                    threadPool,
                                    Actions.always(o -> localClient.execute(CreateFollowerAction.INSTANCE, new CreateFollowerAction.Request(repository, leaderIndexMetaData))),
                                    DEFAULT_TIMEOUT,
                                    ThreadPool.Names.GENERIC, "flush"
                            ));
                    return;
                }
            }

            // 检查Index_UUID是否一致， 不一致则删除并停止同步
            String leaderIndexUUID = leaderIndexMetaData.getIndex().getUUID();
            String registerIndexUUID = indexParams.indexUUID();
            if (registerIndexUUID.equals(leaderIndexUUID) == false) {
                logger.info("[metadata-sync] index name samed but uuid not same, delete local index: {}", index);
                removeLocalIndex(localClient, repository, index);
                return;
            }

            // open/close 状态同步
            IndexMetaData.State leaderIndexState = leaderIndexMetaData.getState();
            IndexMetaData.State followerIndexState = localIndexMetaData.getState();
            if (followerIndexState.equals(leaderIndexState) == false) {
                logger.trace("[metadata-sync] sync index open/close state {}", index, leaderIndexState);
                if (leaderIndexMetaData.getState().equals(IndexMetaData.State.OPEN)) {
                    localClient.admin().indices().prepareOpen(index).execute();
                } else {
                    localClient.admin().indices().prepareClose(index).execute();
                }
                return;
            }

            // 同步 setting, mapping, aliases
            logger.trace("[metadata-sync] sync metadata for index {}", index);
            updateIndexMetaData(localClient, remoteMetaData.index(index), localMetaData.index(index));
        });
    }


    /**
     * 同步 setting, mapping, aliases
     *
     * @param remoteIndexMetaData 本地localIndexMetaData
     * @param localIndexMetaData  远程LeaderIndexMetaData
     */
    private void updateIndexMetaData(Client client, IndexMetaData remoteIndexMetaData, IndexMetaData localIndexMetaData) {

        Function<String, ActionListener> failureHandler = s -> Actions.failure(e -> logger.error("[metadata-sync] update {} failed {} ", s, e));

        String index = remoteIndexMetaData.getIndex().getName();

        // update aliases
        if (!remoteIndexMetaData.getAliases().equals(localIndexMetaData.getAliases())) {
            IndicesAliasesRequest aliasesRequest = compareAlias(index, remoteIndexMetaData.getAliases(), localIndexMetaData.getAliases());
            if (!aliasesRequest.getAliasActions().isEmpty()) {
                client.admin().indices().aliases(aliasesRequest, failureHandler.apply("aliase"));
            }
        }

        // update mappings
        if (!remoteIndexMetaData.getMappings().equals(localIndexMetaData.getMappings())) {
            MappingMetaData mappingMetaData = remoteIndexMetaData.getMappings().iterator().next().value;
            PutMappingRequest putMappingRequest = new PutMappingRequest(index);
            putMappingRequest.type(mappingMetaData.type());
            putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
            client.admin().indices().putMapping(putMappingRequest, failureHandler.apply("mappings"));
        }

        // update settings
        Settings remoteSettings = SettingUtils.filter(remoteIndexMetaData.getSettings());
        Settings localSettings = SettingUtils.filter(localIndexMetaData.getSettings());
        final Settings diffSettings = remoteSettings.filter(s -> (localSettings.get(s) == null || localSettings.get(s).equals(remoteSettings.get(s)) == false));
        Settings dynamicUpdatedSettings = diffSettings.filter(DEFAULT_SCOPED_SETTINGS::isDynamicSetting);
        Settings reopenUpdatedSettings = diffSettings.filter(key -> !DEFAULT_SCOPED_SETTINGS.isDynamicSetting(key));
        if (!dynamicUpdatedSettings.isEmpty()) {
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(dynamicUpdatedSettings);
            client.admin().indices().updateSettings(updateSettingsRequest, failureHandler.apply("settings"));
        }
        if (!reopenUpdatedSettings.isEmpty()) {
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(reopenUpdatedSettings);
            client.admin().indices().close(new CloseIndexRequest(index), success(
                    r -> client.admin().indices().updateSettings(updateSettingsRequest,
                            always(k -> client.admin().indices().prepareOpen(index).execute(failureHandler.apply("settings")))))
            );
        }
    }

    private static IndicesAliasesRequest compareAlias(String index, ImmutableOpenMap<String, AliasMetaData> source, ImmutableOpenMap<String, AliasMetaData> dest) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        source.forEach(e -> {
            if (!dest.containsKey(e.key)) {
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
                aliasAction.index(index).alias(e.value.alias());
                if (e.value.indexRouting() != null) {
                    aliasAction.routing(e.value.indexRouting());
                }
                if (e.value.searchRouting() != null) {
                    aliasAction.searchRouting(e.value.searchRouting());
                }
                if (e.value.filter() != null) {
                    aliasAction.filter(e.value.filter().toString());
                }
                request.addAliasAction(aliasAction);
            }
        });
        dest.forEach(e -> {
            if (!source.containsKey(e.key)) {
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
                aliasAction.index(index).alias(e.value.alias());
                request.addAliasAction(aliasAction);
            }
        });
        return request;
    }

    private void removeLocalIndex(Client client, String repository, String index) {
        DeleteFollowJobAction.Request stopSyncRequest = new DeleteFollowJobAction.Request(repository, index);
        client.execute(DeleteFollowJobAction.INSTANCE, stopSyncRequest).actionGet();
        client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    private boolean leaderIndexAllShardsStarted(Client remoteClient, String index, int shards) {
        ShardStats[] leaderSS;
        try {
            IndicesStatsResponse indicesStatsResponse = remoteClient.admin().indices().prepareStats(index).get();
            leaderSS = indicesStatsResponse.getShards();
        } catch (Exception e) {
            return false;
        }
        long readyPrimaries = Arrays.stream(leaderSS)
                .filter(shardStats -> shardStats.getShardRouting().primary() && shardStats.getShardRouting().state() == ShardRoutingState.STARTED)
                .count();
        if (readyPrimaries != shards) {
            return false;
        }
        return true;
    }

}
