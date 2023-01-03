package org.elasticsearch.apack;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.apack.terms.TermsFetchAction;
import org.elasticsearch.apack.terms.TransportTermsFetchAction;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;


public class JoinPlugin extends Plugin implements SearchPlugin, ActionPlugin {


    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (_client == null) {
            this._client = client;
        }
        return Collections.emptyList();
    }

    @Override
    public List<SearchPlugin.QuerySpec<?>> getQueries() {
        return asList(new SearchPlugin.QuerySpec<>(JoinQueryBuilder.NAME, JoinQueryBuilder::new, JoinQueryBuilder::fromXContent));
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return asList(new ActionPlugin.ActionHandler<>(TermsFetchAction.INSTANCE, TransportTermsFetchAction.class));
    }

    private static Client _client;
    public static Client client() {
        return _client;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return TransportTermsFetchAction.getSettings();
    }

}
