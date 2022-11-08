/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search.agg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.schema.Configuration;
import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.schema.RowSet;
import org.elasticsearch.apack.plain.search.extractor.agg.BucketExtractor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

/**
 * Cursor for composite aggregation (GROUP BY).
 * Stores the query that gets updated/slides across requests.
 */
public class CompositeAggregationCursor implements Cursor {

    private final Logger log = LogManager.getLogger(getClass());

    public static final String NAME = "c";

    private final byte[] nextQuery;
    private final List<BucketExtractor> extractors;
    private final BitSet mask;
    private final int limit;

    CompositeAggregationCursor(byte[] next, List<BucketExtractor> exts, BitSet mask, int remainingLimit) {
        this.nextQuery = next;
        this.extractors = exts;
        this.mask = mask;
        this.limit = remainingLimit;
    }

    public CompositeAggregationCursor(StreamInput in) throws IOException {
        nextQuery = in.readByteArray();
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(BucketExtractor.class);
        mask = BitSet.valueOf(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(nextQuery);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    byte[] next() {
        return nextQuery;
    }

    BitSet mask() {
        return mask;
    }

    List<BucketExtractor> extractors() {
        return extractors;
    }

    int limit() {
        return limit;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener) {
        SearchSourceBuilder q;
        try {
            q = deserializeQuery(registry, nextQuery);
        } catch (Exception ex) {
            listener.onFailure(ex);
            return;
        }

        SearchSourceBuilder query = q;
        SearchRequest search = null;
        client.search(search, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse r) {
                try {
                    // retry
                    if (shouldRetryDueToEmptyPage(r)) {
                        CompositeAggregationCursor.updateCompositeAfterKey(r, search.source());
                        client.search(search, this);
                        return;
                    }
                    CompositeAggsRowSet rowSet = new CompositeAggsRowSet(extractors, mask, r);
                    listener.onResponse(rowSet);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception ex) {
                listener.onFailure(ex);
            }
        });
    }

    public static boolean shouldRetryDueToEmptyPage(SearchResponse response) {
        CompositeAggregation composite = getComposite(response);
        // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
        return composite != null && composite.getBuckets().isEmpty() && composite.afterKey() != null && !composite.afterKey().isEmpty();
    }

    static CompositeAggregation getComposite(SearchResponse response) {
        Aggregation agg = response.getAggregations().get(Aggs.ROOT_GROUP_NAME);
        if (agg == null) {
            return null;
        }

        if (agg instanceof CompositeAggregation) {
            return (CompositeAggregation) agg;
        }

        throw new PlainIllegalArgumentException("Unrecognized root group found; {}", agg.getClass());
    }

    public static boolean updateCompositeAfterKey(SearchResponse r, SearchSourceBuilder next) {
        CompositeAggregation composite = getComposite(r);

        if (composite == null) {
            throw new PlainIllegalArgumentException("Invalid server response; no group-by detected");
        }

        Map<String, Object> afterKey = composite.afterKey();
        // a null after-key means done
        if (afterKey == null) {
            return false;
        }

        AggregationBuilder aggBuilder = next.aggregations().getAggregatorFactories().iterator().next();
        // update after-key with the new value
        if (aggBuilder instanceof CompositeAggregationBuilder) {
            CompositeAggregationBuilder comp = (CompositeAggregationBuilder) aggBuilder;
            comp.aggregateAfter(afterKey);
            return true;
        } else {
            throw new PlainIllegalArgumentException("Invalid client request; expected a group-by but instead got {}", aggBuilder);
        }
    }

    /**
     * Deserializes the search source from a byte array.
     */
    private static SearchSourceBuilder deserializeQuery(NamedWriteableRegistry registry, byte[] source) throws IOException {
        try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(source), registry)) {
            return new SearchSourceBuilder(in);
        }
    }

    /**
     * Serializes the search source to a byte array.
     */
    public static byte[] serializeQuery(SearchSourceBuilder source) throws IOException {
        if (source == null) {
            return new byte[0];
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            source.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }


    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(nextQuery), extractors, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CompositeAggregationCursor other = (CompositeAggregationCursor) obj;
        return Arrays.equals(nextQuery, other.nextQuery)
                && Objects.equals(extractors, other.extractors)
                && Objects.equals(limit, other.limit);
    }

    @Override
    public String toString() {
        return "cursor for composite on index";
    }
}
