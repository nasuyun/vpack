package org.elasticsearch.apack.plain.schema.expression;

import org.elasticsearch.apack.plain.type.DataType;

import java.util.Objects;

public abstract class TypedAttribute extends Attribute {

    private final DataType dataType;

    protected TypedAttribute(String name, DataType dataType) {
        super(name);
        this.dataType = dataType;
    }

    public DataType dataType() {
        return dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(dataType, ((TypedAttribute) obj).dataType);
    }
}
