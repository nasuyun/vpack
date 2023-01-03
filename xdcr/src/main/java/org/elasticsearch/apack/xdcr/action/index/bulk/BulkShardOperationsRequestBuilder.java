package org.elasticsearch.apack.xdcr.action.index.bulk;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class BulkShardOperationsRequestBuilder
        extends ActionRequestBuilder<BulkShardOperationsRequest, BulkShardOperationsResponse, BulkShardOperationsRequestBuilder> {

    public BulkShardOperationsRequestBuilder(final ElasticsearchClient client) {
        super(client, BulkShardOperationsAction.INSTANCE, new BulkShardOperationsRequest());
    }

}
