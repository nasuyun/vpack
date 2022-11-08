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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class TermsFetchRequest extends BroadcastRequest<TermsFetchRequest> {

    @Nullable
    protected String routing;
    @Nullable
    private String preference;
    @Nullable
    private String field;
    private String[] types = Strings.EMPTY_ARRAY;
    private SearchSourceBuilder source;
    private Ordering ordering;
    private Integer maxTermsPerShard;
    private Long expectedTerms;
    private long nowInMillis;

    public TermsFetchRequest() {
        source = new SearchSourceBuilder();
    }


    public TermsFetchRequest(String[] indices) {
        this.indices(indices);
        source = new SearchSourceBuilder();
    }

    /**
     * Validates the request
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (field == null) {
            validationException = addValidationError("index field is missing", validationException);
        }
        if (source != null && source.trackTotalHits() == false) {
            validationException = addValidationError("disabling [track_total_hits] is not allowed in a scroll context", validationException);
        }
        if (source != null && source.from() > 0) {
            validationException = addValidationError("using [from] is not allowed in a scroll context", validationException);
        }
        return validationException;
    }


    public SearchSourceBuilder source() {
        return source;
    }

    public void source(SearchSourceBuilder source) {
        this.source = source;
    }

    /**
     * The field to extract terms from.
     */
    public String field() {
        return field;
    }

    /**
     * The field to extract terms from.
     */
    public TermsFetchRequest field(String field) {
        this.field = field;
        return this;
    }


    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public String[] types() {
        return this.types;
    }

    /**
     * The types of documents the query will run against. Defaults to all types.
     */
    public TermsFetchRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public TermsFetchRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The current time in milliseconds
     */
    public long nowInMillis() {
        return nowInMillis;
    }

    /**
     * Sets the current time in milliseconds
     */
    public TermsFetchRequest nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
        return this;
    }

    /**
     * The routing values to control the shards that the request will be executed on.
     */
    public TermsFetchRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * The preference value to control what node the request will be executed on
     */
    public TermsFetchRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    /**
     * The current preference value
     */
    public String preference() {
        return this.preference;
    }

    /**
     * The types of ordering
     */
    public enum Ordering {
        DEFAULT,
        DOC_SCORE
    }

    /**
     * Sets the ordering to use before performing the term cut.
     */
    public TermsFetchRequest orderBy(Ordering ordering) {
        this.ordering = ordering;
        return this;
    }

    /**
     * Returns the ordering to use before performing the term cut.
     */
    public Ordering getOrderBy() {
        return ordering;
    }

    /**
     * The max number of terms to gather per shard
     */
    public TermsFetchRequest maxTermsPerShard(Integer maxTermsPerShard) {
        this.maxTermsPerShard = maxTermsPerShard;
        return this;
    }

    /**
     * The max number of terms to gather per shard
     */
    public Integer maxTermsPerShard() {
        return maxTermsPerShard;
    }

    /**
     * The number of expected terms to collect across all shards.
     */
    public TermsFetchRequest expectedTerms(Long expectedTerms) {
        this.expectedTerms = expectedTerms;
        return this;
    }

    /**
     * The number of expected terms to collect across all shards.
     */
    public Long expectedTerms() {
        return expectedTerms;
    }


    /**
     * Deserialize
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.readBoolean()) {
            routing = in.readOptionalString();
        }

        if (in.readBoolean()) {
            preference = in.readOptionalString();
        }

        if (in.readBoolean()) {
            source = in.readOptionalWriteable(SearchSourceBuilder::new);
        }

        if (in.readBoolean()) {
            types = in.readStringArray();
        }

        field = in.readString();
        nowInMillis = in.readVLong();

        if (in.readBoolean()) {
            ordering = Ordering.values()[in.readVInt()];
        }

        if (in.readBoolean()) {
            maxTermsPerShard = in.readVInt();
        }

        if (in.readBoolean()) {
            expectedTerms = in.readVLong();
        }

    }

    /**
     * Serialize
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (routing != null) {
            out.writeBoolean(true);
            out.writeOptionalString(routing);
        } else {
            out.writeBoolean(false);
        }

        if (preference != null) {
            out.writeBoolean(true);
            out.writeOptionalString(preference);
        } else {
            out.writeBoolean(false);
        }

        if (source != null) {
            out.writeBoolean(true);
            out.writeOptionalWriteable(source);
        } else {
            out.writeBoolean(false);
        }

        if (types == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(types);
        }

        out.writeString(field);
        out.writeVLong(nowInMillis);

        if (ordering == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(ordering.ordinal());
        }

        if (maxTermsPerShard == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(maxTermsPerShard);
        }

        if (expectedTerms == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVLong(expectedTerms);
        }
    }

    /**
     * String representation of the request
     */
    @Override
    public String toString() {
        return Arrays.toString(indices) + Arrays.toString(types) + "[" + field + "], source[" + source + "]";
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
