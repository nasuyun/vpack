package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.search.ResultRowSet;
import org.elasticsearch.apack.plain.search.extractor.AggsExtractor;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.BitSet;
import java.util.List;

public class AggsRowSet extends ResultRowSet<AggsExtractor> {

    private final int size;
    private int row = 0;
    protected final Aggregations aggregations;

    public AggsRowSet(List<AggsExtractor> exts, BitSet mask, Aggregations aggregations) {
        super(exts, mask);
        size = exts.size();
        this.aggregations = aggregations;
    }

    @Override
    protected Object extractValue(AggsExtractor aggsExtractor) {
        return aggsExtractor.extract(aggregations);
    }

    @Override
    protected boolean doHasCurrent() {
        return row < size;
    }

    @Override
    protected boolean doNext() {
        if (row < size - 1) {
            row++;
            return true;
        }
        return false;
    }

    @Override
    protected void doReset() {
        row = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Cursor nextPageCursor() {
        return Cursor.EMPTY;
    }

}
