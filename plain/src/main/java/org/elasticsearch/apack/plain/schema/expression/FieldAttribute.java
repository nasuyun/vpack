package org.elasticsearch.apack.plain.schema.expression;

import org.elasticsearch.apack.plain.type.DataType;
import org.elasticsearch.apack.plain.type.EsField;
import org.elasticsearch.apack.plain.util.StringUtils;

import java.util.Objects;

/**
 * Attribute for an ES field.
 * To differentiate between the different type of fields this class offers:
 * - name - the fully qualified name (foo.bar.tar)
 * - path - the path pointing to the field name (foo.bar)
 * - parent - the immediate parent of the field; useful for figuring out the type of field (nested vs object)
 * - nestedParent - if nested, what's the parent (which might not be the immediate one)
 */
public class FieldAttribute extends TypedAttribute {

    private final FieldAttribute parent;
    private final FieldAttribute nestedParent;
    private final String path;
    private final EsField field;

    public FieldAttribute(String name, EsField field) {
        this(null, name, field);
    }


    public FieldAttribute(FieldAttribute parent, String name, EsField field) {
        super(name, field.getDataType());
        this.path = parent != null ? parent.name() : StringUtils.EMPTY;
        this.parent = parent;
        this.field = field;

        // figure out the last nested parent
        FieldAttribute nestedPar = null;
        if (parent != null) {
            nestedPar = parent.nestedParent;
            if (parent.dataType() == DataType.NESTED) {
                nestedPar = parent;
            }
        }
        this.nestedParent = nestedPar;
    }


    public FieldAttribute parent() {
        return parent;
    }

    public String path() {
        return path;
    }

    public boolean isNested() {
        return nestedParent != null;
    }

    public FieldAttribute nestedParent() {
        return nestedParent;
    }

    public EsField.Exact getExactInfo() {
        return field.getExactInfo();
    }

    public FieldAttribute exactAttribute() {
        EsField exactField = field.getExactField();
        if (exactField.equals(field) == false) {
            return innerField(exactField);
        }
        return this;
    }

    private FieldAttribute innerField(EsField type) {
        return new FieldAttribute(this, name() + "." + type.getName(), type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(path, ((FieldAttribute) obj).path);
    }


    public EsField field() {
        return field;
    }
}
