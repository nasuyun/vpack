/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search;

public abstract class FieldReference implements FieldExtraction {
    /**
     * Field name.
     *
     * @return field name.
     */
    public abstract String name();

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return false;
    }
}
