package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class ClearRestoreSessionRequestBuilder extends ActionRequestBuilder<ClearRestoreSessionRequest,
        ClearRestoreSessionAction.ClearRestoreSessionResponse, ClearRestoreSessionRequestBuilder> {

    ClearRestoreSessionRequestBuilder(ElasticsearchClient client) {
        super(client, ClearRestoreSessionAction.INSTANCE, new ClearRestoreSessionRequest());
    }
}
