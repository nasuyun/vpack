package org.elasticsearch.apack.plain.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.apack.plain.ShowColumns;
import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.schema.resovler.IndexResolver;
import org.elasticsearch.apack.plain.search.SearchHitsExtra;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportPlainSearchAction extends HandledTransportAction<PlainSearchRequest, PlainSearchResponse> {

    private final Client client;
    private final ClusterService clusterService;
    private final IndexResolver indexResolver;

    @Inject
    public TransportPlainSearchAction(Settings settings, ThreadPool threadPool,
                                      Client client,
                                      ClusterService clusterService,
                                      TransportService transportService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PlainSearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, PlainSearchRequest::new);
        this.client = client;
        this.clusterService = clusterService;
        this.indexResolver = new IndexResolver(client, clusterService.getClusterName().value());
    }

    @Override
    protected void doExecute(PlainSearchRequest request, ActionListener<PlainSearchResponse> listener) {
        String[] requiredColumns = request.columns();
        // 获取schema
        ShowColumns showColumns = new ShowColumns(indexResolver, indexNameExpressionResolver, clusterService);
        showColumns.onSchema(request.searchRequest(), requiredColumns, ActionListener.wrap(
                fields -> {
                    // 底层extra 如开启docvalue用docvalue获取
                    for (Attribute field : fields) {
                        if (field.dataType().defaultDocValues) {
                            request.searchRequest().source().docValueField(field.name());
                        }
                    }

                    SearchHitsExtra searchHitsExtra = new SearchHitsExtra(fields, request.zoneId());
                    ActionListener<SearchResponse> hitExtraListener = ActionListener.wrap(
                            response -> {
                                PlainSearchResponse plainSearchResponse = searchHitsExtra.extraResponse(response, request.searchRequest().source().size());
                                listener.onResponse(plainSearchResponse);
                            },
                            listener::onFailure);
//                    AggExtra aggExtra = new AggExtra(request.zoneId(), request.searchRequest(), request.searchRequest().source().size());
//                    ActionListener<SearchResponse> aggExtraListener = ActionListener.wrap(response -> {
//                        PlainSearchResponse plainSearchResponse = aggExtra.extraResponse(response, request.searchRequest().source().size());
//                        listener.onResponse(plainSearchResponse);
//                    }, listener::onFailure);

                    client.search(request.searchRequest(), hitExtraListener);
                },
                listener::onFailure
        ));
    }

}
