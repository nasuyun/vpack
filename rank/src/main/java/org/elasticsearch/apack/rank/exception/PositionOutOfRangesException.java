package org.elasticsearch.apack.rank.exception;

import org.elasticsearch.ElasticsearchException;

public class PositionOutOfRangesException extends ElasticsearchException {

    public PositionOutOfRangesException(String s) {
        super(s);
    }

}