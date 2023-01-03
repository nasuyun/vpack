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
