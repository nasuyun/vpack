/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.apack.plain.type.Schema;

class EmptyRowSet extends AbstractRowSet implements SchemaRowSet {
    private final Schema schema;

    EmptyRowSet(Schema schema) {
        this.schema = schema;
    }

    @Override
    protected boolean doHasCurrent() {
        return false;
    }

    @Override
    protected boolean doNext() {
        return false;
    }

    @Override
    protected Object getColumn(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doReset() {
        // no-op
    }

    @Override
    public int size() {
        return 0;
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
