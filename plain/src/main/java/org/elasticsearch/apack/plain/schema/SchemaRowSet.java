/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.schema;


import org.elasticsearch.apack.plain.type.Schema;

public interface SchemaRowSet extends RowSet {
    /**
     * Schema for the results.
     */
    Schema schema();

    @Override
    default int columnCount() {
        return schema().size();
    }
}
