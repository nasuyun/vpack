package org.elasticsearch.apack.rank.exception;

import org.elasticsearch.ElasticsearchException;

public class ParseException extends ElasticsearchException {

    public ParseException(String msg, Object... args) {
        super(msg, args);
    }
}
