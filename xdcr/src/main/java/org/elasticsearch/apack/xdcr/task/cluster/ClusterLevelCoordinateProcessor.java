package org.elasticsearch.apack.xdcr.task.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.apack.xdcr.action.index.DeleteFollowJobAction;
import org.elasticsearch.apack.xdcr.action.index.PutFollowJobAction;
import org.elasticsearch.apack.xdcr.utils.RemoteConnections;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.apack.xdcr.task.TaskUtil.indices;
import static org.elasticsearch.apack.xdcr.utils.Ssl.*;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;

public class ClusterLevelCoordinateProcessor {

    private static final Logger logger = LogManager.getLogger(ClusterLevelCoordinateProcessor.class);
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(5);

    private final Client client;
    private final String repository;
    private final String includes;
    private final String excludes;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterLevelCoordinateTaskParams params;

    ClusterLevelCoordinateProcessor(ClusterService clusterService, Client client, ClusterLevelCoordinateTaskParams params) {
        this.client = client;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(client.settings());
        this.params = params;
        this.repository = params.repository();
        this.includes = params.includes();
        this.excludes = params.excludes();
    }

    public void process() {
        logger.trace("[cluster-coordinate] started cluster level coordinate [{}]", repository);
        if (RemoteConnections.isConnected(client, repository) == false) {
            logger.info("[cluster-coordinate] remote cluster [{}] not connected, process skipped", repository);
            return;
        }
        logger.trace("[cluster-coordinate] started");
        Client remoteClient = systemClient(remoteClient(client, repository));
        ClusterState leaderClusterState;
        try {
            leaderClusterState = remoteClient.admin()
                    .cluster().prepareState()
                    .clear().setMetaData(true).get(TimeValue.timeValueSeconds(60)).getState();
        } catch (Exception e) {
            logger.warn("[cluster-coordinate] process failed {}", e.getMessage());
            return;
        }

        /**
         * includes 为空时匹配所有索引
         * excludes 为空时不匹配任何索引
         */
        String[] inclusions = Strings.splitStringByCommaToArray(includes);
        String[] exclusions = Strings.splitStringByCommaToArray(excludes);
        Set<String> incloudIndices = inclusions.length == 0 ?
                indexNameExpressionResolver.resolveExpressions(leaderClusterState, "*") :
                indexNameExpressionResolver.resolveExpressions(leaderClusterState, inclusions);
        Set<String> excloudIndices = exclusions.length == 0 ?
                Collections.emptySet() :
                indexNameExpressionResolver.resolveExpressions(leaderClusterState, exclusions);

        // 获取远程集群索引列表，过滤只有 index.soft_deleted 开启的索引
        ImmutableOpenMap<String, Settings> indicesSettings = getSettings(leaderClusterState, inclusions).getIndexToSettings();
        List<String> remoteIndices = StreamSupport.stream(indicesSettings.spliterator(), false)
                .filter(s -> INDEX_SOFT_DELETES_SETTING.get(s.value) == true)
                .map(c -> c.key)
                .collect(Collectors.toList());

        // 获取本地已托管同步的索引
        List<String> localIndices = indices(clusterService.state(), repository);
        for (String index : remoteIndices) {
            if (localIndices.contains(index)) {
                // 本地已存在，如果需要被排除
                if (excloudIndices.contains(index)) {
                    // 停止任务 不做删除
                    logger.info("[cluster-coordinate] stop remote index sync,  repository[{}] index[{}]", repository, index);
                    userClient(client, params.headers()).execute(DeleteFollowJobAction.INSTANCE, new DeleteFollowJobAction.Request(repository, index)).actionGet(TIMEOUT);
                }
            } else {
                // 本地不存在，如果不在排除范围内且存在匹配的索引
                if (excloudIndices.contains(index) == false && incloudIndices.contains(index)) {
                    // 启动任务
                    logger.info("[cluster-coordinate] start sync index [{}] on repository[{}]", index, repository);
                    userClient(client, params.headers()).execute(PutFollowJobAction.INSTANCE, new PutFollowJobAction.Request(repository, index)).actionGet(TIMEOUT);
                }
            }
        }
    }

    private GetSettingsResponse getSettings(ClusterState state, String... indexExpressions) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, IndicesOptions.lenientExpandOpen(), indexExpressions);
        ImmutableOpenMap.Builder<String, Settings> indexToSettingsBuilder = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> indexToDefaultSettingsBuilder = ImmutableOpenMap.builder();
        for (Index concreteIndex : concreteIndices) {
            IndexMetaData indexMetaData = state.getMetaData().index(concreteIndex);
            if (indexMetaData == null) {
                continue;
            }
            Settings indexSettings = indexMetaData.getSettings();
            indexToSettingsBuilder.put(concreteIndex.getName(), indexSettings);
        }
        return new GetSettingsResponse(indexToSettingsBuilder.build(), indexToDefaultSettingsBuilder.build());
    }

}
