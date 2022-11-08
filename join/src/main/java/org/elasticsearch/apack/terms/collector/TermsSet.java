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
package org.elasticsearch.apack.terms.collector;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * A set of terms.
 */
public abstract class TermsSet<T> implements Streamable {

    protected final CircuitBreaker breaker;

    protected TermsSet(final CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    protected abstract void add(T o);

    public abstract void merge(TermsSet terms);

    public abstract int size();

    public abstract void readFrom(StreamInput in) throws IOException;

    public abstract void writeTo(StreamOutput out) throws IOException;

    public abstract BytesRef writeToBytes();

    public abstract TermsEncoding getEncoding();

    public abstract boolean contains(T term);

    public abstract void release();

    public boolean isEmpty() {
        return size() == 0;
    }

    public static TermsSet newTermsSet(long expectedElements, TermsEncoding termsEncoding, CircuitBreaker breaker) {
        switch (termsEncoding) {
            case LONG:
                return new LongTermsSet(expectedElements, breaker);
            case INTEGER:
                return new IntegerTermsSet(expectedElements, breaker);
            case BYTES:
                return new BytesRefTermsSet(breaker);
            default:
                throw new IllegalArgumentException("[termsByQuery] Invalid terms encoding: " + termsEncoding.name());
        }
    }

    public static TermsSet readFrom(BytesRef in) {
        TermsEncoding termsEncoding = TermsEncoding.values()[Bytes.readInt(in)];
        switch (termsEncoding) {
            case INTEGER:
                return new IntegerTermsSet(in);
            case LONG:
                return new LongTermsSet(in);
            case BYTES:
                return new BytesRefTermsSet(in);
            default:
                throw new IllegalArgumentException("[termsQuery] Invalid terms encoding: " + termsEncoding.name());
        }
    }

}
