
package org.elasticsearch.apack.plain.type;


import org.elasticsearch.apack.plain.util.Check;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;

public class Schema implements Iterable<Schema.Entry> {

    public interface Entry {
        String name();
        DataType type();
    }

    static class DefaultEntry implements Entry {
        private final String name;
        private final DataType type;

        DefaultEntry(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public DataType type() {
            return type;
        }
    }

    public static final Schema EMPTY = new Schema(emptyList(), emptyList());
    
    private final List<String> names;
    private final List<DataType> types;

    public Schema(List<String> names, List<DataType> types) {
        Check.isTrue(names.size() == types.size(), "Different # of names {} vs types {}", names, types);
        this.types = types;
        this.names = names;
    }

    public List<String> names() {
        return names;
    }

    public List<DataType> types() {
        return types;
    }

    public int size() {
        return names.size();
    }

    public Entry get(int i) {
        return new DefaultEntry(names.get(i), types.get(i));
    }

    public DataType type(String name) {
        int indexOf = names.indexOf(name);
        if (indexOf < 0) {
            return null;
        }
        return types.get(indexOf);
    }

    @Override
    public Iterator<Entry> iterator() {
        return new Iterator<Entry>() {
            private final int size = size();
            private int pos = -1;
            
            @Override
            public boolean hasNext() {
                return pos < size - 1;
            }

            @Override
            public Entry next() {
                if (pos++ >= size) {
                    throw new NoSuchElementException();
                }
                return get(pos);
            }
        };
    }

    public Stream<Entry> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public Spliterator<Entry> spliterator() {
        return Spliterators.spliterator(iterator(), size(), 0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(names.get(i));
            sb.append(":");
            sb.append(types.get(i).typeName);
        }
        sb.append("]");
        return sb.toString();
    }
}
