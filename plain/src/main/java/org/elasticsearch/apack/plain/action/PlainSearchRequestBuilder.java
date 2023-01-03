package org.elasticsearch.apack.plain.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A search action request builder.
 */
public class PlainSearchRequestBuilder extends ActionRequestBuilder<PlainSearchRequest, PlainSearchResponse, PlainSearchRequestBuilder> {

    public PlainSearchRequestBuilder(ElasticsearchClient client, PlainSearchAction action) {
        super(client, action, new PlainSearchRequest());
    }
}
