/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search.extractor.agg;

import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.search.agg.GroupByRef.Property;
import org.elasticsearch.apack.plain.util.DateUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

public class CompositeKeyExtractor implements BucketExtractor {

    /**
     * Key or Composite extractor.
     */
    static final String NAME = "k";

    private final String key;
    private final Property property;
    private final ZoneId zoneId;
    private final boolean isDateTimeBased;

    /**
     * Constructs a new <code>CompositeKeyExtractor</code> instance.
     */
    public CompositeKeyExtractor(String key, Property property, ZoneId zoneId, boolean isDateTimeBased) {
        this.key = key;
        this.property = property;
        this.zoneId = zoneId;
        this.isDateTimeBased = isDateTimeBased;
    }

    CompositeKeyExtractor(StreamInput in) throws IOException {
        key = in.readString();
        property = in.readEnum(Property.class);
        zoneId = ZoneId.of(in.readString());
        isDateTimeBased = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeEnum(property);
        out.writeString(zoneId.getId());
        out.writeBoolean(isDateTimeBased);
    }

    String key() {
        return key;
    }

    Property property() {
        return property;
    }

    ZoneId zoneId() {
        return zoneId;
    }

    public boolean isDateTimeBased() {
        return isDateTimeBased;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(Bucket bucket) {
        if (property == Property.COUNT) {
            return bucket.getDocCount();
        }
        // get the composite value
        Object m = bucket.getKey();

        if (!(m instanceof Map)) {
            throw new PlainIllegalArgumentException("Unexpected bucket returned: {}", m);
        }

        Object object = ((Map<?, ?>) m).get(key);

        if (isDateTimeBased) {
            if (object == null) {
                return object;
            } else if (object instanceof Long) {
                object = DateUtils.asDateTime(((Long) object).longValue(), zoneId);
            } else {
                throw new PlainIllegalArgumentException("Invalid date key returned: {}", object);
            }
        }

        return object;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, property, zoneId, isDateTimeBased);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        CompositeKeyExtractor other = (CompositeKeyExtractor) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(property, other.property)
                && Objects.equals(zoneId, other.zoneId)
                && Objects.equals(isDateTimeBased, other.isDateTimeBased);
    }

    @Override
    public String toString() {
        return "|" + key + "|";
    }

}
