package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

class EmptyCursor implements Cursor {
    static final String NAME = "0";
    static final EmptyCursor INSTANCE = new EmptyCursor();

    private EmptyCursor() {
        // Only one instance allowed
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener) {
        throw new IllegalArgumentException("there is no next page");
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        // There is nothing to clean
        listener.onResponse(false);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public int hashCode() {
        return 27;
    }

    @Override
    public String toString() {
        return "no next page";
    }
}
