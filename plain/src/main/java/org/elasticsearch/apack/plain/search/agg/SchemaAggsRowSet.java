package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.schema.SchemaRowSet;
import org.elasticsearch.apack.plain.search.agg.AggsRowSet;
import org.elasticsearch.apack.plain.search.extractor.AggsExtractor;
import org.elasticsearch.apack.plain.type.Schema;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

import java.util.BitSet;
import java.util.List;

public class SchemaAggsRowSet extends AggsRowSet implements SchemaRowSet {

    private final Schema schema;
    private final int size;
    private int row = 0;
    private List<? extends Bucket> buckets;

    public SchemaAggsRowSet(Schema schema, List<AggsExtractor> exts, BitSet mask, Aggregations aggregations) {
        super(exts, mask, aggregations);
        this.schema = schema;
        this.size = 1;
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

    @Override
    public Schema schema() {
        return schema;
    }
}
