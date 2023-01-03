
package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.search.extractor.agg.BucketExtractor;
import org.elasticsearch.apack.plain.search.ResultRowSet;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;

import java.util.BitSet;
import java.util.List;

import static java.util.Collections.emptyList;


class CompositeAggsRowSet extends ResultRowSet<BucketExtractor> {

    private final List<? extends CompositeAggregation.Bucket> buckets;

    private final Cursor cursor;

    private final int size;
    private int row = 0;

    CompositeAggsRowSet(List<BucketExtractor> exts, BitSet mask, SearchResponse response) {
        super(exts, mask);

        CompositeAggregation composite = CompositeAggregationCursor.getComposite(response);
        if (composite != null) {
            buckets = composite.getBuckets();
        } else {
            buckets = emptyList();
        }

        // page size
//        size = limit == -1 ? buckets.size() : Math.min(buckets.size(), limit);
        size = exts.size();
        cursor = Cursor.EMPTY;
    }

    @Override
    protected Object extractValue(BucketExtractor e) {
        return e.extract(buckets.get(row));
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
        return cursor;
    }
}
