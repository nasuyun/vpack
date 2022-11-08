/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.schema.SchemaRowSet;
import org.elasticsearch.apack.plain.search.extractor.agg.BucketExtractor;
import org.elasticsearch.apack.plain.type.Schema;

import java.util.BitSet;
import java.util.List;


public class SchemaCompositeAggsRowSet extends CompositeAggsRowSet implements SchemaRowSet {

    private final Schema schema;

    public SchemaCompositeAggsRowSet(Schema schema, List<BucketExtractor> exts, BitSet mask, SearchResponse response) {
        super(exts, mask, response);
        this.schema = schema;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
