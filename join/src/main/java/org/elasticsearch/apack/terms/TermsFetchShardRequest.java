package org.elasticsearch.apack.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal terms by query request executed directly against a specific index shard.
 */
public class TermsFetchShardRequest extends BroadcastShardRequest {

    private TermsFetchRequest request;
    private AliasFilter filteringAliases;

    /**
     * Default constructor
     */
    public TermsFetchShardRequest() {
    }


    public TermsFetchShardRequest(ShardId shardId, AliasFilter filteringAliases, TermsFetchRequest request) {
        super(shardId, request);
        this.request = request;
        this.filteringAliases = Objects.requireNonNull(filteringAliases, "filteringAliases must not be null");
    }

    public TermsFetchRequest request() {
        return request;
    }

    public String field() {
        return request.field();
    }

    public String[] types() {
        return request.types();
    }

    public SearchSourceBuilder source() {
        return request.source();
    }

    public long nowInMillis() {
        return request.nowInMillis();
    }

    public AliasFilter filteringAliases() {
        return filteringAliases;
    }

    /**
     * Deserialize
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = new TermsFetchRequest();
        request.readFrom(in);

        if (in.readBoolean()) {
            filteringAliases = new AliasFilter(in);
        }
    }

    /**
     * Serialize
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);

        if (filteringAliases == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            filteringAliases.writeTo(out);
        }
    }

}
