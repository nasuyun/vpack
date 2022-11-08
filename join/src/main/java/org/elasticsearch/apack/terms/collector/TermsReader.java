package org.elasticsearch.apack.terms.collector;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

public class TermsReader {

    /**
     * For TermsQuery Converted
     */
    public static List readList(BytesRef in) {
        TermsEncoding termsEncoding = TermsEncoding.values()[Bytes.readInt(in)];
        switch (termsEncoding) {
            case INTEGER:
                return newIntList(in);
            case LONG:
                return newLongList(in);
            case BYTES:
                return newBytesRefList(in);
            default:
                throw new IllegalArgumentException("[termsQuery] Invalid terms encoding: " + termsEncoding.name());
        }
    }

    private static List newIntList(BytesRef bytes) {
        int size = Bytes.readInt(bytes);
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            list.add(Bytes.readVInt(bytes));
        }
        return list;
    }

    private static List newLongList(BytesRef bytes) {
        int size = Bytes.readInt(bytes);
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            list.add(Bytes.readLong(bytes));
        }
        return list;
    }

    private static List newBytesRefList(BytesRef bytes) {
        int size = Bytes.readInt(bytes);
        List list = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            BytesRef reusable = new BytesRef();
            Bytes.readBytesRef(bytes, reusable);
            list.add(reusable);
        }
        return list;
    }
}
