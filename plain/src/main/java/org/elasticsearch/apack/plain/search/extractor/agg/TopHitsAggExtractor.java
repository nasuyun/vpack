/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search.extractor.agg;

import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.type.DataType;
import org.elasticsearch.apack.plain.util.DateUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

public class TopHitsAggExtractor implements BucketExtractor {

    static final String NAME = "th";

    private final String name;
    private final DataType fieldDataType;
    private final ZoneId zoneId;

    public TopHitsAggExtractor(String name, DataType fieldDataType, ZoneId zoneId) {
        this.name = name;
        this.fieldDataType = fieldDataType;
        this.zoneId = zoneId;
    }

    TopHitsAggExtractor(StreamInput in) throws IOException {
        name = in.readString();
        fieldDataType = in.readEnum(DataType.class);
        zoneId = ZoneId.of(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(fieldDataType);
        out.writeString(zoneId.getId());
    }

    String name() {
        return name;
    }

    DataType fieldDataType() {
        return fieldDataType;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(Bucket bucket) {
        InternalTopHits agg = bucket.getAggregations().get(name);
        if (agg == null) {
            throw new PlainIllegalArgumentException("Cannot find an aggregation named {}", name);
        }

        if (agg.getHits() == null || agg.getHits().getTotalHits() == 0) {
            return null;
        }

        Object value = agg.getHits().getAt(0).getFields().values().iterator().next().getValue();
        if (fieldDataType.isDateBased()) {
            return DateUtils.asDateTime(Long.parseLong(value.toString()), zoneId);
        } else {
            return value;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldDataType, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TopHitsAggExtractor other = (TopHitsAggExtractor) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(fieldDataType, other.fieldDataType)
            && Objects.equals(zoneId, other.zoneId);
    }

    @Override
    public String toString() {
        return "TopHits>" + name + "[" + fieldDataType + "]@" + zoneId;
    }
}
