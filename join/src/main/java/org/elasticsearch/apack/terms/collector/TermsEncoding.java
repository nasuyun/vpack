package org.elasticsearch.apack.terms.collector;

public enum TermsEncoding {

    LONG(0), INTEGER(1), BLOOM(2), BYTES(3);

    private final byte value;

    TermsEncoding(int value) {
        this.value = (byte) value;
    }

    public byte value() {
        return value;
    }
}
