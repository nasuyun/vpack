package org.elasticsearch.apack.plain.schema.expression;

import org.elasticsearch.apack.plain.type.DataType;

abstract public class Attribute  {

    final String name;

    public Attribute(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public abstract DataType dataType();

}
