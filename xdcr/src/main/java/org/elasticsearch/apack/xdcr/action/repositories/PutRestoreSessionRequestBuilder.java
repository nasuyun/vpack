package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class PutRestoreSessionRequestBuilder extends ActionRequestBuilder<PutRestoreSessionRequest,
        PutRestoreSessionAction.PutRestoreSessionResponse, PutRestoreSessionRequestBuilder> {

    PutRestoreSessionRequestBuilder(ElasticsearchClient client) {
        super(client, PutRestoreSessionAction.INSTANCE, new PutRestoreSessionRequest());
    }
}
