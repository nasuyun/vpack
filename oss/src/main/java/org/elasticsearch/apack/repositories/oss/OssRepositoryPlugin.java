/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.apack.repositories.oss;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.apack.repositories.oss.auto.AutoSnapshoExecutor;
import org.elasticsearch.apack.repositories.oss.auto.AutoSnapshotTaskParams;
import org.elasticsearch.apack.repositories.oss.auto.CreateAutoSnapshotAction;
import org.elasticsearch.apack.repositories.oss.auto.DeleteAutoSnapshotAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.*;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.*;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

/**
 * A plugin to add a repository type that writes to and from the Azure cloud storage service.
 */
public class OssRepositoryPlugin extends Plugin implements ActionPlugin, RepositoryPlugin, PersistentTaskPlugin {

    public OssRepositoryPlugin(Settings settings) {
    }

    private SetOnce<Client> clientSetOnce = new SetOnce<>();
    private SetOnce<ThreadPool> threadPoolSetOnce = new SetOnce<>();

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(CreateAutoSnapshotAction.Action.INSTANCE, CreateAutoSnapshotAction.Transport.class),
                new ActionHandler<>(DeleteAutoSnapshotAction.Action.INSTANCE, DeleteAutoSnapshotAction.Transport.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new CreateAutoSnapshotAction.RestCreateAutoSnapshot(settings, restController),
                new DeleteAutoSnapshotAction.RestDeleteAutoSnapshot(settings, restController)
        );

    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        clientSetOnce.set(client);
        threadPoolSetOnce.set(threadPool);
        return Collections.emptyList();
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            SettingsModule settingsModule) {
        return Arrays.asList(new AutoSnapshoExecutor(clusterService, threadPool, client));
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(OssRepository.TYPE,
                (metadata) -> new OssRepository(metadata, env, namedXContentRegistry));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, AutoSnapshotTaskParams.NAME, AutoSnapshotTaskParams::new)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(AutoSnapshotTaskParams.NAME), AutoSnapshotTaskParams::fromXContent)
        );
    }


}
