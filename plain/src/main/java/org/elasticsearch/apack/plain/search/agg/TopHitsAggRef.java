package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.apack.plain.type.DataType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Reference to a TopHits aggregation.
 * Since only one field is returned we only need its data type
 */
public class TopHitsAggRef extends AggRef {

    // only for readability via toString()
    private final String name;
    private final DataType fieldDataType;

    public TopHitsAggRef(String name, DataType fieldDataType) {
        this.name = name;
        this.fieldDataType = fieldDataType;
    }

    public String name() {
        return name;
    }

    public DataType fieldDataType() {
        return fieldDataType;
    }

    @Override
    public String toString() {
        return ">" + name + "[" + fieldDataType.typeName + "]";
    }

    @Override
    public void collectFields(SearchSourceBuilder sourceBuilder) {

    }
}
