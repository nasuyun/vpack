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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.apack.terms.collector.TermsEncoding;
import org.elasticsearch.apack.terms.collector.TermsSet;

import java.io.IOException;
import java.util.List;

/**
 * The response of the terms by query action.
 */
public class TermsFetchResponse extends BroadcastResponse implements StatusToXContentObject {

    /**
     * The set of terms that has been retrieved
     */
    private BytesRef encodedTerms;

    /**
     * The number of terms
     */
    private int size;

    /**
     * The type of encoding used
     */
    private TermsEncoding termsEncoding;

    /**
     * Has the terms set been pruned ?
     */
    private boolean isPruned;

    /**
     * How long it took to retrieve the terms.
     */
    private long tookInMillis;

    /**
     * Default constructor
     */
    TermsFetchResponse() {
    }

    TermsFetchResponse(long tookInMillis, int totalShards, int successfulShards, int failedShards,
                       List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
    }

    /**
     * Main constructor
     *
     * @param termsSet         the merged terms
     * @param tookInMillis     the time in millis it took to retrieve the terms.
     * @param totalShards      the number of shards the request executed on
     * @param successfulShards the number of shards the request executed on successfully
     * @param failedShards     the number of failed shards
     * @param shardFailures    the failures
     */
    TermsFetchResponse(TermsSet termsSet, boolean isPruned, long tookInMillis, int totalShards, int successfulShards, int failedShards,
                       List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.isPruned = isPruned;
        this.encodedTerms = termsSet.writeToBytes();
        this.termsEncoding = termsSet.getEncoding();
        this.size = termsSet.size();
        this.tookInMillis = tookInMillis;
    }

    /**
     * Gets the time it took to execute the terms by query action.
     */
    public long getTookInMillis() {
        return this.tookInMillis;
    }

    /**
     * Gets the merged terms
     *
     * @return the terms
     */
    public BytesRef getEncodedTermsSet() {
        return encodedTerms;
    }

    /**
     * Gets the number of terms
     */
    public int size() {
        return size;
    }

    /**
     * Returns true if the set of terms has been pruned.
     */
    public boolean isPruned() {
        return isPruned;
    }

    /**
     * Deserialize
     *
     * @param in the input
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        isPruned = in.readBoolean();
        size = in.readVInt();
        termsEncoding = TermsEncoding.values()[in.readVInt()];
        encodedTerms = in.readBytesRef();
    }

    /**
     * Serialize
     *
     * @param out the output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        // Encode flag
        out.writeBoolean(isPruned);
        // Encode size
        out.writeVInt(size);
        // Encode type of encoding
        out.writeVInt(termsEncoding.ordinal());
        // Encode terms
        out.writeBytesRef(encodedTerms);
        // Release terms
        encodedTerms = null;
    }

    @Override
    public RestStatus status() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("termsQuery").value("finished");
        builder.field("size").value(this.size);
        builder.field("took(ms)").value(tookInMillis);
        builder.endObject();
        return builder;
    }
}
