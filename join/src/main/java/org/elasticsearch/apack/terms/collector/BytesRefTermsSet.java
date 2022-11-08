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

import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A set of bytes ref terms.
 */
public class BytesRefTermsSet extends TermsSet<BytesRef> {

    private transient Counter bytesUsed;
    private transient ByteBlockPool pool;
    private transient BytesRefHash set;

    private static final int HEADER_SIZE = 9;

    public BytesRefTermsSet(final CircuitBreaker breaker) {
        super(breaker);
        this.bytesUsed = Counter.newCounter();
        this.pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
        this.set = new BytesRefHash(pool);
    }

    public BytesRefTermsSet(BytesRef bytes) {
        super(null);
        this.readFromBytes(bytes);
    }

    public void add(BytesRef term) {
        this.set.add(term);
    }

    @Override
    public boolean contains(BytesRef term) {
        return this.set.find(term) != -1;
    }

    @Override
    public void merge(TermsSet terms) {
        if (!(terms instanceof BytesRefTermsSet)) {
            throw new UnsupportedOperationException("Invalid type: BytesRefTermsSet expected.");
        }

        BytesRefHash input = ((BytesRefTermsSet) terms).set;
        BytesRef reusable = new BytesRef();
        for (int i = 0; i < input.size(); i++) {
            input.get(i, reusable);
            set.add(reusable);
        }
    }

    public BytesRefHash getBytesRefHash() {
        return set;
    }

    @Override
    public int size() {
        return this.set.size();
    }

    /**
     * Return the memory usage of this object in bytes.
     */
    public long ramBytesUsed() {
        return bytesUsed.get();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        bytesUsed = Counter.newCounter();
        pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
        set = new BytesRefHash(pool);
        for (long i = 0; i < size; i++) {
            set.add(in.readBytesRef());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Encode size of list
        out.writeInt(set.size());
        // Encode BytesRefs
        BytesRef reusable = new BytesRef();
        for (int i = 0; i < this.set.size(); i++) {
            this.set.get(i, reusable);
            out.writeBytesRef(reusable);
        }
    }

    @Override
    public BytesRef writeToBytes() {
        int size = set.size();
        BytesRef bytes = new BytesRef(new byte[HEADER_SIZE + (int) bytesUsed.get()]);
        // Encode encoding type
        Bytes.writeInt(bytes, this.getEncoding().value());
        // Encode size of the set
        Bytes.writeInt(bytes, size);
        // Encode longs
        BytesRef reusable = new BytesRef();
        for (int i = 0; i < this.set.size(); i++) {
            this.set.get(i, reusable);
            Bytes.writeBytesRef(reusable, bytes);
        }
        bytes.length = bytes.offset;
        bytes.offset = 0;
        return bytes;
    }

    private void readFromBytes(BytesRef bytes) {
        // Read size fo the set
        int size = Bytes.readInt(bytes);

        // Read terms
        bytesUsed = Counter.newCounter();
        pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
        set = new BytesRefHash(pool);

        BytesRef reusable = new BytesRef();
        for (int i = 0; i < size; i++) {
            Bytes.readBytesRef(bytes, reusable);
            set.add(reusable);
        }
    }

    @Override
    public TermsEncoding getEncoding() {
        return TermsEncoding.BYTES;
    }

    @Override
    public void release() {
        if (set != null) {
            set.close();
        }
    }

}
