package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.type.DataType;
import org.elasticsearch.apack.plain.type.Schema;
import org.elasticsearch.apack.plain.util.Check;

import java.util.ArrayList;
import java.util.List;

public abstract class Rows {

    public static Schema schema(List<Attribute> attr) {
        List<String> names = new ArrayList<>(attr.size());
        List<DataType> types = new ArrayList<>(attr.size());

        for (Attribute a : attr) {
            names.add(a.name());
            types.add(a.dataType());
        }
        return new Schema(names, types);
    }

    public static SchemaRowSet of(List<Attribute> attrs, List<List<?>> values) {
        if (values.isEmpty()) {
            return empty(attrs);
        }

        if (values.size() == 1) {
            return singleton(attrs, values.get(0).toArray());
        }

        Schema schema = schema(attrs);
        return new ListRowSet(schema, values);
    }

    public static SchemaRowSet singleton(List<Attribute> attrs, Object... values) {
        return singleton(schema(attrs), values);
    }

    public static SchemaRowSet singleton(Schema schema, Object... values) {
        Check.isTrue(schema.size() == values.length, "Schema {} and values {} are out of sync", schema, values);
        return new SingletonRowSet(schema, values);
    }

    public static SchemaRowSet empty(Schema schema) {
        return new EmptyRowSet(schema);
    }

    public static SchemaRowSet empty(List<Attribute> attrs) {
        return new EmptyRowSet(schema(attrs));
    }
}
