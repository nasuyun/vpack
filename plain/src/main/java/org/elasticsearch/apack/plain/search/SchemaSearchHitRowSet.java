
package org.elasticsearch.apack.plain.search;

import org.elasticsearch.apack.plain.schema.SchemaRowSet;
import org.elasticsearch.apack.plain.search.extractor.HitExtractor;
import org.elasticsearch.apack.plain.type.Schema;
import org.elasticsearch.search.SearchHit;


import java.util.BitSet;
import java.util.List;


public class SchemaSearchHitRowSet extends SearchHitRowSet implements SchemaRowSet {
    private final Schema schema;

    public SchemaSearchHitRowSet(Schema schema, List<HitExtractor> exts, BitSet mask, SearchHit[] hits, int limitHits, String scrollId) {
        super(exts, mask, hits, limitHits, scrollId);
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
