
package org.elasticsearch.apack.plain.type;

import java.util.Collections;
import java.util.Objects;

/**
 * SQL-related information about an index field that cannot be supported by SQL
 */
public class UnsupportedEsField extends EsField {

    private String originalType;

    public UnsupportedEsField(String name, String originalType) {
        super(name, DataType.UNSUPPORTED, Collections.emptyMap(), false);
        this.originalType = originalType;
    }

    public String getOriginalType() {
        return originalType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        UnsupportedEsField that = (UnsupportedEsField) o;
        return Objects.equals(originalType, that.originalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originalType);
    }
}
