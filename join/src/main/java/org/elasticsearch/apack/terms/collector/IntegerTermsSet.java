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
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

public class IntegerTermsSet extends NumericTermsSet<Integer> {

    private transient IntHashSet set;

    private static final int HEADER_SIZE = 9;

    public IntegerTermsSet(final CircuitBreaker breaker) {
        super(breaker);
        this.set = new CircuitBreakerIntHashSet(0);
    }

    public IntegerTermsSet(final long expectedElements, final CircuitBreaker breaker) {
        super(breaker);
        this.set = new CircuitBreakerIntHashSet(Math.toIntExact(expectedElements));
    }

    public IntegerTermsSet(BytesRef bytes) {
        super(null);
        this.readFromBytes(bytes);
    }

    @Override
    public void add(Integer term) {
        this.set.add(term);
    }

    @Override
    public boolean contains(Integer term) {
        return this.set.contains(term);
    }

    @Override
    public void merge(TermsSet terms) {
        if (!(terms instanceof IntegerTermsSet)) {
            throw new UnsupportedOperationException("Invalid type: IntegerTermsSet expected.");
        }
        this.set.addAll(((IntegerTermsSet) terms).set);
    }

    @Override
    public int size() {
        return this.set.size();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();
        set = new CircuitBreakerIntHashSet(size);
        for (long i = 0; i < size; i++) {
            set.add(in.readVInt());
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

        // Encode ints
        BytesRef buffer = new BytesRef(new byte[1024 * 8]);
        Iterator<IntCursor> it = set.iterator();
        while (it.hasNext()) {
            Bytes.writeVInt(buffer, it.next().value);
            if (buffer.offset > buffer.bytes.length - 5) {
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
        BytesRef bytesRef = new BytesRef(new byte[HEADER_SIZE + size * 5]);
        Bytes.writeInt(bytesRef, this.getEncoding().value());
        Bytes.writeInt(bytesRef, size);
        for (IntCursor i : set) {
            Bytes.writeVInt(bytesRef, i.value);
        }
        bytesRef.length = bytesRef.offset;
        bytesRef.offset = 0;
        return bytesRef;
    }

    private void readFromBytes(BytesRef bytesRef) {
        int size = Bytes.readInt(bytesRef);
        set = new IntScatterSet(size);
        for (int i = 0; i < size; i++) {
            set.add(Bytes.readVInt(bytesRef));
        }
    }

    @Override
    public TermsEncoding getEncoding() {
        return TermsEncoding.INTEGER;
    }

    @Override
    public void release() {
        if (set != null) {
            set.release();
        }
    }

    private final class CircuitBreakerIntHashSet extends IntHashSet {

        public CircuitBreakerIntHashSet(int expectedElements) {
            super(expectedElements);
        }

        @Override
        protected void allocateBuffers(int arraySize) {
            long newMemSize = (arraySize + 1) * 4l; // array size + emtpyElementSlot
            long oldMemSize = keys == null ? 0 : keys.length * 4l;

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
            }
        }

        @Override
        public void release() {
            long memSize = keys == null ? 0 : keys.length * 4l;

            // Release - do not allocate a new minimal buffer
            assigned = 0;
            hasEmptyKey = false;
            keys = null;

            // Adjust breaker
            breaker.addWithoutBreaking(-memSize);
        }

    }

}
