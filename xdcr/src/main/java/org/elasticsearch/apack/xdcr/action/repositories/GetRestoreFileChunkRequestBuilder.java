package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class GetRestoreFileChunkRequestBuilder extends ActionRequestBuilder<GetRestoreFileChunkRequest,
        GetRestoreFileChunkAction.GetRestoreFileChunkResponse, GetRestoreFileChunkRequestBuilder> {

    GetRestoreFileChunkRequestBuilder(ElasticsearchClient client) {
        super(client, GetRestoreFileChunkAction.INSTANCE, new GetRestoreFileChunkRequest());
    }
}
