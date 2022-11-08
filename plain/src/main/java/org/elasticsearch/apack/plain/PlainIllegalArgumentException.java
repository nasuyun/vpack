package org.elasticsearch.apack.plain;

import org.elasticsearch.ElasticsearchException;

public class PlainIllegalArgumentException extends ElasticsearchException {

    public PlainIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public PlainIllegalArgumentException(String message, Object... args) {
        super(message, args);
    }

    public PlainIllegalArgumentException(String message) {
        super(message);
    }
}
