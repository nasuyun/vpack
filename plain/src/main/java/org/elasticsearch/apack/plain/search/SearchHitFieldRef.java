
package org.elasticsearch.apack.plain.search;

import org.elasticsearch.apack.plain.search.FieldReference;
import org.elasticsearch.apack.plain.type.DataType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchHitFieldRef extends FieldReference {
    private final String name;
    private final DataType dataType;
    private final boolean docValue;
    private final String hitName;

    public SearchHitFieldRef(String name, DataType dataType, boolean useDocValueInsteadOfSource) {
        this(name, dataType, useDocValueInsteadOfSource, null);
    }

    public SearchHitFieldRef(String name, DataType dataType, boolean useDocValueInsteadOfSource, String hitName) {
        this.name = name;
        this.dataType = dataType;
        this.docValue = useDocValueInsteadOfSource;
        this.hitName = hitName;
    }

    public String hitName() {
        return hitName;
    }

    @Override
    public String name() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean useDocValue() {
        return docValue;
    }

    @Override
    public void collectFields(SearchSourceBuilder sourceBuilder) {
        // nested fields are handled by inner hits
        if (hitName != null) {
            return;
        }
        if (docValue) {
            //TODO
            //sourceBuilder.addDocField(name, dataType.format());
        } else {
            //sourceBuilder.addSourceField(name);
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
