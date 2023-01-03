package org.elasticsearch.apack.plain.type;

import java.util.Map;

public class DateEsField extends EsField {

    public DateEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, DataType.DATETIME, properties, hasDocValues);
    }
}
