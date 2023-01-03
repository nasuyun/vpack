package org.elasticsearch.apack.plain.search;

public abstract class FieldReference implements FieldExtraction {
    /**
     * Field name.
     *
     * @return field name.
     */
    public abstract String name();

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return false;
    }
}
