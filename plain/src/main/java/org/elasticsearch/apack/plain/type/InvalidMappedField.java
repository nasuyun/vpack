

package org.elasticsearch.apack.plain.type;

import java.util.Objects;

import static java.util.Collections.emptyMap;

/**
 * Representation of field mapped differently across indices.
 * Used during mapping discovery only.
 */
public class InvalidMappedField extends EsField {

    private final String errorMessage;

    public InvalidMappedField(String name, String errorMessage) {
        super(name, DataType.UNSUPPORTED, emptyMap(), false);
        this.errorMessage = errorMessage;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }
        
        return false;
    }

    @Override
    public EsField getExactField() {
        throw new IllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");

    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }
}
