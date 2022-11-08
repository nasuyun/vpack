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

import com.carrotsearch.hppc.BufferAllocationException;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongScatterSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

public class LongTermsSet extends NumericTermsSet<Long> {

    private transient LongHashSet set;

    private static final int HEADER_SIZE = 9;

    public LongTermsSet(final CircuitBreaker breaker) {
        super(breaker);
        this.set = new CircuitBreakerLongHashSet(0);
    }

    public LongTermsSet(final long expectedElements, final CircuitBreaker breakerService) {
        super(breakerService);
        this.set = new CircuitBreakerLongHashSet(Math.toIntExact(expectedElements));
    }

    public LongTermsSet(BytesRef bytes) {
        super(null);
        this.readFromBytes(bytes);
    }

    @Override
    public void merge(TermsSet terms) {
        if (!(terms instanceof LongTermsSet)) {
            throw new UnsupportedOperationException("Invalid type: LongTermSet expected.");
        }
        this.set.addAll(((LongTermsSet) terms).set);
    }

    /**
     * Used by unit-tests
     */
    public LongHashSet getLongHashSet() {
        return set;
    }

    @Override
    public void add(Long term) {
        this.set.add(term);
    }

    @Override
    public boolean contains(Long term) {
        return this.set.contains(term);
    }

    @Override
    public int size() {
        return this.set.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        set = new CircuitBreakerLongHashSet(size);
        for (long i = 0; i < size; i++) {
            set.add(in.readLong());
        }
    }

    /**
     * Serialize the list of terms to the {@link StreamOutput}.
     * <br>
     * Given the low performance of {@link org.elasticsearch.common.io.stream.BytesStreamOutput} when writing a large number
     * of longs (5 to 10 times slower than writing directly to a byte[]), we use a small buffer of 8kb
     * to optimise the throughput. 8kb seems to be the optimal buffer size, larger buffer size did not improve
     * the throughput.
     *
     * @param out the output
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {

        // Encode size of list
        out.writeInt(set.size());

        // Encode longs
        BytesRef buffer = new BytesRef(new byte[1024 * 8]);
        Iterator<LongCursor> it = set.iterator();
        while (it.hasNext()) {
            Bytes.writeLong(buffer, it.next().value);
            if (buffer.offset == buffer.length) {
                out.write(buffer.bytes, 0, buffer.offset);
                buffer.offset = 0;
            }
        }

        // flush the remaining bytes from the buffer
        out.write(buffer.bytes, 0, buffer.offset);
    }

    @Override
    public BytesRef writeToBytes() {
        int size = set.size();

        BytesRef bytes = new BytesRef(new byte[HEADER_SIZE + 8 * size]);

        // Encode encoding type
        Bytes.writeInt(bytes, this.getEncoding().value());

        // Encode size of the set
        Bytes.writeInt(bytes, size);

        // Encode longs
        for (LongCursor i : set) {
            Bytes.writeLong(bytes, i.value);
        }

        bytes.length = bytes.offset;
        bytes.offset = 0;
        return bytes;
    }

    private void readFromBytes(BytesRef bytes) {

        // Read size fo the set
        int size = Bytes.readInt(bytes);

        // Read terms

        // Scatter set is slightly more efficient than the hash set, but should be used only for lookups,
        // not for merging
        set = new LongScatterSet(size);
        for (int i = 0; i < size; i++) {
            set.add(Bytes.readLong(bytes));
        }
    }

    @Override
    public TermsEncoding getEncoding() {
        return TermsEncoding.LONG;
    }

    @Override
    public void release() {
        if (set != null) {
            set.release();
        }
    }

    private Iterator<Long> iterator = new Iterator<Long>() {
        @Override
        public boolean hasNext() {
            return set.iterator().hasNext();
        }

        @Override
        public Long next() {
            return set.iterator().next().value;
        }
    };

    /**
     * A {@link LongHashSet} integrated with the {@link CircuitBreaker}. It will adjust the circuit breaker
     * for every new call to {@link #allocateBuffers(int)}.
     * <p>
     * This set must not be reused after a call to {@link #release()}.
     */
    private final class CircuitBreakerLongHashSet extends LongHashSet {

        public CircuitBreakerLongHashSet(int expectedElements) {
            super(expectedElements);
        }

        @Override
        protected void allocateBuffers(int arraySize) {
            long newMemSize = (arraySize + 1) * 8l; // array size + emptyElementSlot
            long oldMemSize = keys == null ? 0 : keys.length * 8l;

            // Adjust the breaker with the new memory size
            breaker.addEstimateBytesAndMaybeBreak(newMemSize, "<terms_set>");

            try {
                // Allocate the new buffer
                super.allocateBuffers(arraySize);
                // Adjust the breaker by removing old memory size
                breaker.addWithoutBreaking(-oldMemSize);
            } catch (BufferAllocationException e) {
                // If the allocation failed, remove
                breaker.addWithoutBreaking(-newMemSize);
                throw e;
            }
        }

        @Override
        public void release() {
            long memSize = keys == null ? 0 : keys.length * 8l;

            // Release - do not allocate a new minimal buffer
            assigned = 0;
            hasEmptyKey = false;
            keys = null;

            // Adjust breaker
            breaker.addWithoutBreaking(-memSize);
        }

    }

}
