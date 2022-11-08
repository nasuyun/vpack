/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 * <p>
 * This file is part of the SIREn project.
 * <p>
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * <p>
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, .
 */
package org.elasticsearch.apack.terms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * A terms by query action request builder. This is an internal api.
 */
public class TermsFetchRequestBuilder extends BroadcastOperationRequestBuilder<TermsFetchRequest, TermsFetchResponse, TermsFetchRequestBuilder> {

    public TermsFetchRequestBuilder(ElasticsearchClient client, TermsFetchAction action) {
        super(client, action, new TermsFetchRequest());
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public TermsFetchRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public TermsFetchRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public TermsFetchRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public TermsFetchRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The field to extract terms from.
     */
    public TermsFetchRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    public TermsFetchRequestBuilder setSource(SearchSourceBuilder source) {
        request.source(source);
        return this;
    }

    /**
     * The ordering to use before performing the term cut.
     */
    public TermsFetchRequestBuilder setOrderBy(TermsFetchRequest.Ordering ordering) {
        request.orderBy(ordering);
        return this;
    }

    /**
     * The max number of terms collected per shard
     */
    public TermsFetchRequestBuilder setMaxTermsPerShard(int maxTermsPerShard) {
        request.maxTermsPerShard(maxTermsPerShard);
        return this;
    }


    /**
     * The number of expected terms to collect across all shards.
     */
    public TermsFetchRequestBuilder setExpectedTerms(long expectedTerms) {
        request.expectedTerms(expectedTerms);
        return this;
    }

    @Override
    public void execute(ActionListener<TermsFetchResponse> listener) {
        client.execute(TermsFetchAction.INSTANCE, request, listener);
    }

}
