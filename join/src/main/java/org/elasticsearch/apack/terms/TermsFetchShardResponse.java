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

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.apack.terms.collector.*;

import java.io.IOException;

/**
 * Internal terms by query response of a shard terms by query request executed directly against a specific shard.
 */
class TermsFetchShardResponse extends BroadcastShardResponse {

    private TermsSet termsSet;
    private final CircuitBreaker breaker;
    private boolean pruned;

    /**
     * Default constructor
     */
    TermsFetchShardResponse(final CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /**
     * Main constructor
     *
     * @param shardId  the id of the shard the request executed on
     * @param termsSet the terms gathered from the shard
     */
    public TermsFetchShardResponse(ShardId shardId, TermsSet termsSet, boolean pruned, CircuitBreaker breaker) {
        super(shardId);
        this.termsSet = termsSet;
        this.breaker = breaker;
        this.pruned = pruned;
    }

    /**
     * Gets the gathered terms.
     *
     * @return the {@link TermsSet}
     */
    public TermsSet getTerms() {
        return this.termsSet;
    }

    public boolean isPruned() {
        return pruned;
    }

    /**
     * Deserialize
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        pruned = in.readBoolean();
        TermsEncoding termsEncoding = TermsEncoding.values()[in.readVInt()];
        switch (termsEncoding) {

            case LONG:
                termsSet = new LongTermsSet(breaker);
                termsSet.readFrom(in);
                return;

            case INTEGER:
                termsSet = new IntegerTermsSet(breaker);
                termsSet.readFrom(in);
                return;

            case BYTES:
                termsSet = new BytesRefTermsSet(breaker);
                termsSet.readFrom(in);
                return;

            default:
                throw new IOException("[termsByQuery] Invalid type of terms encoding: " + termsEncoding.name());

        }
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        try {
            super.writeTo(out);
            out.writeBoolean(pruned);
            out.writeVInt(termsSet.getEncoding().ordinal());
            termsSet.writeTo(out);
        } finally {
            termsSet.release();
        }
    }
}
