/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.apack.plain.type.Schema;

import java.util.List;

public class ListRowSet extends AbstractRowSet implements SchemaRowSet {

    private final Schema schema;
    private final List<List<?>> list;
    private int pos = 0;

    protected ListRowSet(Schema schema, List<List<?>> list) {
        this.schema = schema;
        this.list = list;
    }

    @Override
    protected boolean doHasCurrent() {
        return pos < size();
    }

    @Override
    protected boolean doNext() {
        if (pos + 1 < size()) {
            pos++;
            return true;
        }
        return false;
    }

    @Override
    protected Object getColumn(int index) {
        return list.get(pos).get(index);
    }

    @Override
    protected void doReset() {
        pos = 0;
    }

    @Override
    public int size() {
        return list.size();
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
