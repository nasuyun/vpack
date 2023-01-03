package org.elasticsearch.apack.plain.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class PlainSearchAction extends Action<PlainSearchRequest, PlainSearchResponse, PlainSearchRequestBuilder> {

    public static final PlainSearchAction INSTANCE = new PlainSearchAction();
    public static final String NAME = "indices:data/read/search/format";

    private PlainSearchAction() {
        super(NAME);
    }

    @Override
    public PlainSearchResponse newResponse() {
        return new PlainSearchResponse();
    }

    @Override
    public PlainSearchRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PlainSearchRequestBuilder(client, this);
    }
}
