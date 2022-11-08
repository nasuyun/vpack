/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.schema;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import java.time.ZoneId;
import java.time.ZonedDateTime;

// Typed object holding properties for a given query
public class Configuration {

    private final ZoneId zoneId;

    private final TimeValue timeout;

    @Nullable
    private QueryBuilder filter;

    public Configuration(ZoneId zoneId, TimeValue timeout) {
        this.zoneId = zoneId;
        this.timeout = timeout;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public TimeValue timeout() {
        return timeout;
    }
}
