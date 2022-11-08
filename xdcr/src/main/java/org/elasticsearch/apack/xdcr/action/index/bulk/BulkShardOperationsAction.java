/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.action.index.bulk;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

public class BulkShardOperationsAction
        extends Action<BulkShardOperationsRequest, BulkShardOperationsResponse, BulkShardOperationsRequestBuilder> {

    public static final BulkShardOperationsAction INSTANCE = new BulkShardOperationsAction();
    public static final String NAME = "internal:xdcr/bulk_shard_operations[s]";

    private BulkShardOperationsAction() {
        super(NAME);
    }

    @Override
    public BulkShardOperationsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new BulkShardOperationsRequestBuilder(client);
    }

    @Override
    public BulkShardOperationsResponse newResponse() {
        return new BulkShardOperationsResponse();
    }

}
