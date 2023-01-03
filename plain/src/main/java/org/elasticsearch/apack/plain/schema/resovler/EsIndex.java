package org.elasticsearch.apack.plain.schema.resovler;

import org.elasticsearch.apack.plain.type.EsField;

import java.util.Map;

public class EsIndex {

    private final String name;
    private final Map<String, EsField> mapping;

    public EsIndex(String name, Map<String, EsField> mapping) {
        assert name != null;
        assert mapping != null;
        this.name = name;
        this.mapping = mapping;
    }

    public String name() {
        return name;
    }

    public Map<String, EsField> mapping() {
        return mapping;
    }

    @Override
    public String toString() {
        return name;
    }
}
