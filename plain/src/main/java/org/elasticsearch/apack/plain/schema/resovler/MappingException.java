package org.elasticsearch.apack.plain.schema.resovler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

public class MappingException extends ElasticsearchException {

    public MappingException(String message, Object... args) {
        super(message, args);
    }

    public MappingException(String message, Throwable ex) {
        super(message, ex);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
