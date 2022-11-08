/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.plain.search;

import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.schema.AbstractRowSet;
import org.elasticsearch.common.io.stream.NamedWriteable;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class ResultRowSet<E extends NamedWriteable> extends AbstractRowSet {

    private final List<E> extractors;
    private final BitSet mask;

    protected ResultRowSet(List<E> extractors, BitSet mask) {
        this.extractors = extractors;
        this.mask = mask;
        //Check.isTrue(mask.length() <= extractors.size(), "Invalid number of extracted columns specified");
    }

    @Override
    public final int columnCount() {
        return mask.cardinality();
    }

    @Override
    protected Object getColumn(int column) {
        return extractValue(userExtractor(column));
    }

    List<E> extractors() {
        return extractors;
    }

    BitSet mask() {
        return mask;
    }

    E userExtractor(int column) {
        int i = -1;
        // find the nth set bit
        for (i = mask.nextSetBit(0); i >= 0; i = mask.nextSetBit(i + 1)) {
            if (column-- == 0) {
                return extractors.get(i);
            }
        }

        throw new PlainIllegalArgumentException("Cannot find column [{}]", column);
    }

    Object resultColumn(int column) {
        return extractValue(extractors().get(column));
    }

    int resultColumnCount() {
        return extractors.size();
    }

    public void forEachResultColumn(Consumer<? super Object> action) {
        Objects.requireNonNull(action);
        int rowSize = resultColumnCount();
        for (int i = 0; i < rowSize; i++) {
            action.accept(resultColumn(i));
        }
    }
    

    protected abstract Object extractValue(E e);
}
