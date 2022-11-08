/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy notNull the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.apack;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.apack.xdcr.action.cluster.StartClusterSyncAction;
import org.elasticsearch.apack.xdcr.action.cluster.StopClusterSyncAction;
import org.elasticsearch.apack.xdcr.action.index.*;
import org.elasticsearch.apack.xdcr.action.index.bulk.BulkShardOperationsAction;
import org.elasticsearch.apack.xdcr.action.index.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.apack.xdcr.action.repositories.*;
import org.elasticsearch.apack.xdcr.action.stats.StateAction;
import org.elasticsearch.apack.xdcr.action.stats.StatsAction;
import org.elasticsearch.apack.xdcr.repository.RemoteRepository;
import org.elasticsearch.apack.xdcr.repository.RestoreSourceService;
import org.elasticsearch.apack.xdcr.rest.*;
import org.elasticsearch.apack.xdcr.task.cluster.ClusterLevelCoordinateExecutor;
import org.elasticsearch.apack.xdcr.task.cluster.ClusterLevelCoordinateTaskParams;
import org.elasticsearch.apack.xdcr.task.index.IndexSyncExecutor;
import org.elasticsearch.apack.xdcr.task.index.IndexSyncTaskParams;
import org.elasticsearch.apack.xdcr.task.metadata.MetadataCoordinateExecutor;
import org.elasticsearch.apack.xdcr.task.metadata.MetadataCoordinateService;
import org.elasticsearch.apack.xdcr.task.metadata.MetadataCoordinateTaskParams;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.*;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.apack.XDCRSettings.*;

public class XDCRPlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin, EnginePlugin, RepositoryPlugin {


    private Client client;
    private final boolean enabled;
    private final Settings settings;
    private final SetOnce<RestoreSourceService> restoreSourceService = new SetOnce<>();
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();
    private final SetOnce<XDCRSettings> xdcrSettings = new SetOnce<>();

    public XDCRPlugin(final Settings settings) {
        this.settings = settings;
        this.enabled = XDCR_ENABLED_SETTING.get(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {

        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new ActionHandler<>(PutFollowJobAction.INSTANCE, PutFollowJobAction.Transport.class),
                new ActionHandler<>(DeleteFollowJobAction.INSTANCE, DeleteFollowJobAction.Transport.class),
                new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
                new ActionHandler<>(BulkShardOperationsAction.INSTANCE, TransportBulkShardOperationsAction.class),
                new ActionHandler<>(StartClusterSyncAction.INSTANCE, StartClusterSyncAction.Transport.class),
                new ActionHandler<>(StopClusterSyncAction.INSTANCE, StopClusterSyncAction.Transport.class),
                new ActionHandler<>(MetaDataSyncStartAction.INSTANCE, MetaDataSyncStartAction.Transport.class),
                new ActionHandler<>(MetaDataSyncStopAction.INSTANCE, MetaDataSyncStopAction.Transport.class),
                new ActionHandler<>(RemoteRecoveryAction.INSTANCE, RemoteRecoveryAction.Transport.class),
                new ActionHandler<>(CreateFollowerAction.INSTANCE, CreateFollowerAction.Transport.class),
                new ActionHandler<>(StatsAction.INSTANCE, StatsAction.Transport.class),
                new ActionHandler<>(StateAction.INSTANCE, StateAction.Transport.class),
                new ActionHandler<>(ClearRestoreSessionAction.INSTANCE, ClearRestoreSessionAction.TransportDeleteRestoreSessionAction.class),
                new ActionHandler<>(DeleteInternalRepositoryAction.INSTANCE, DeleteInternalRepositoryAction.TransportDeleteInternalRepositoryAction.class),
                new ActionHandler<>(GetRestoreFileChunkAction.INSTANCE, GetRestoreFileChunkAction.TransportGetRestoreFileChunkAction.class),
                new ActionHandler<>(PutInternalRepositoryAction.INSTANCE, PutInternalRepositoryAction.TransportPutInternalRepositoryAction.class),
                new ActionHandler<>(PutRestoreSessionAction.INSTANCE, PutRestoreSessionAction.TransportPutRestoreSessionAction.class),
                new ActionHandler<>(ShardSeqNoAction.INSTANCE, ShardSeqNoAction.TransportShardSegNoAction.class)
        );
    }


    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new RestStatsAction(settings, restController),
                new RestStateAction(settings, restController),
                new RestStatsCatAction(settings, restController),
                new RestStartIndexSyncAction(settings, restController),
                new RestStopIndexSyncAction(settings, restController),
                new RestStartClusterSyncAction(settings, restController),
                new RestStopClusterSyncAction(settings, restController),
                new RestRemoteRecoveryAction(settings, restController)
        );

    }

    @Override
    public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry) {

        if (!enabled) {
            return emptyList();
        }

        // 同步metadata
        new MetadataCoordinateService(client, threadPool);

        this.threadPool.set(threadPool);
        this.client = client;
        this.xdcrSettings.set(new XDCRSettings(settings, clusterService.getClusterSettings()));
        XDCRRepositoryManager repositoryManager = new XDCRRepositoryManager(settings, clusterService, (NodeClient) client);
        RestoreSourceService restoreSourceService = new RestoreSourceService(threadPool, xdcrSettings.get());
        this.restoreSourceService.set(restoreSourceService);
        return Arrays.asList(
                repositoryManager,
                restoreSourceService
        );
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule) {
        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new ClusterLevelCoordinateExecutor(xdcrSettings.get(), clusterService, threadPool, client),
                new IndexSyncExecutor(clusterService, threadPool, client),
                new MetadataCoordinateExecutor(clusterService, threadPool, client)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {

        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, IndexSyncTaskParams.NAME, IndexSyncTaskParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, MetadataCoordinateTaskParams.NAME, MetadataCoordinateTaskParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ClusterLevelCoordinateTaskParams.NAME, ClusterLevelCoordinateTaskParams::new)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {

        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(IndexSyncTaskParams.NAME), IndexSyncTaskParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(MetadataCoordinateTaskParams.NAME), MetadataCoordinateTaskParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ClusterLevelCoordinateTaskParams.NAME), ClusterLevelCoordinateTaskParams::fromXContent)
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (!enabled) {
            return emptyList();
        }
        return Arrays.asList(
                new FixedExecutorBuilder(settings, XDCR_THREAD_POOL_CLUSTER_COORDINATE, 1, -1, "vpack.thread_pool.xdcr.cluster.coordinate"),
                new FixedExecutorBuilder(settings, XDCR_THREAD_POOL_MEATADATA_COORDINATE, 1, -1, "vpack.thread_pool.xdcr.metadata.coordinate"),
                new FixedExecutorBuilder(settings, XDCR_THREAD_POOL_BULK, 32, 32, "vpack.thread_pool.xdcr.bulk")
        );
    }

    @Override
    public Map<String, Repository.Factory> getInternalRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        if (!enabled) {
            return emptyMap();
        }

        Repository.Factory repositoryFactory = (metadata) -> new RemoteRepository(metadata, client, settings, xdcrSettings.get(), threadPool.get());
        return Collections.singletonMap(RemoteRepository.TYPE, repositoryFactory);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (!enabled) {
            return;
        }

        indexModule.addIndexEventListener(this.restoreSourceService.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return XDCRSettings.getSettings();
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        // 不用follower engine 可直接切换
        return Optional.empty();
//        if (XDCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
//            return Optional.of(new FollowingEngineFactory());
//        } else {
//            return Optional.empty();
//        }
    }

}
