/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

class PutRestoreSessionRequestBuilder extends ActionRequestBuilder<PutRestoreSessionRequest,
        PutRestoreSessionAction.PutRestoreSessionResponse, PutRestoreSessionRequestBuilder> {

    PutRestoreSessionRequestBuilder(ElasticsearchClient client) {
        super(client, PutRestoreSessionAction.INSTANCE, new PutRestoreSessionRequest());
    }
}
