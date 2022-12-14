package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteInternalRepositoryRequest extends ActionRequest {

    private final String name;

    public DeleteInternalRepositoryRequest(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("DeleteInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("DeleteInternalRepositoryRequest cannot be serialized for sending across the wire.");
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteInternalRepositoryRequest that = (DeleteInternalRepositoryRequest) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DeleteInternalRepositoryRequest{" +
            "name='" + name + '\'' +
            '}';
    }
}
