
package org.elasticsearch.apack.plain.type;

import org.elasticsearch.common.collect.Tuple;

import java.util.Map;
import java.util.function.Function;

/**
 * SQL-related information about an index field with text type
 */
public class TextEsField extends EsField {

    public TextEsField(String name, Map<String, EsField> properties, boolean hasDocValues) {
        super(name, DataType.TEXT, properties, hasDocValues);
    }

    @Override
    public EsField getExactField() {
        Tuple<EsField, String> findExact = findExact();
        if (findExact.v1() == null) {
            throw new IllegalArgumentException(findExact.v2());
        }
        return findExact.v1();
    }

    @Override
    public Exact getExactInfo() {
        return PROCESS_EXACT_FIELD.apply(findExact());
    }

    private Tuple<EsField, String> findExact() {
        EsField field = null;
        for (EsField property : getProperties().values()) {
            if (property.getDataType() == DataType.KEYWORD && property.getExactInfo().hasExact()) {
                if (field != null) {
                    return new Tuple<>(null, "Multiple exact keyword candidates available for [" + getName() +
                        "]; specify which one to use");
                }
                field = property;
            }
        }
        if (field == null) {
            return new Tuple<>(null, "No keyword/multi-field defined exact matches for [" + getName() +
                "]; define one or use MATCH/QUERY instead");
        }
        return new Tuple<>(field, null);
    }

    private Function<Tuple<EsField, String>, Exact> PROCESS_EXACT_FIELD = tuple -> {
        if (tuple.v1() == null) {
            return new Exact(false, tuple.v2());
        } else {
            return new Exact(true, null);
        }
    };
}
