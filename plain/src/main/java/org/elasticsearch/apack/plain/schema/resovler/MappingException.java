/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
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
