
package org.elasticsearch.apack.plain.util;

/**
 * Simply utility class used for setting a state, typically
 * for closures (which require outside variables to be final).
 */
public class Holder<T> {

    private T value = null;

    public Holder() {
    }

    public Holder(T value) {
        this.value = value;
    }

    public void set(T value) {
        this.value = value;
    }

    public T get() {
        return value;
    }
}
