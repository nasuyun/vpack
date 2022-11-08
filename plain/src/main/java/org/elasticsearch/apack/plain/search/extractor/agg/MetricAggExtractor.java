/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.plain.search.extractor.agg;

import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.util.DateUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.apack.plain.search.agg.Aggs.ROOT_GROUP_NAME;

public class MetricAggExtractor implements BucketExtractor {

    static final String NAME = "m";

    private final String name;
    private final String property;
    private final String innerKey;
    private final boolean isDateTimeBased;
    private final ZoneId zoneId;

    public MetricAggExtractor(String name, String property, String innerKey, ZoneId zoneId, boolean isDateTimeBased) {
        this.name = name;
        this.property = property;
        this.innerKey = innerKey;
        this. isDateTimeBased =isDateTimeBased;
        this.zoneId = zoneId;
    }

    MetricAggExtractor(StreamInput in) throws IOException {
        name = in.readString();
        property = in.readString();
        innerKey = in.readOptionalString();
        isDateTimeBased = in.readBoolean();
        zoneId = ZoneId.of(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(property);
        out.writeOptionalString(innerKey);
        out.writeBoolean(isDateTimeBased);
        out.writeString(zoneId.getId());
    }

    String name() {
        return name;
    }

    String property() {
        return property;
    }

    String innerKey() {
        return innerKey;
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
        InternalAggregation agg = bucket.getAggregations().get(name);
        if (agg == null) {
            throw new PlainIllegalArgumentException("Cannot find an aggregation named {}", name);
        }

        if (!containsValues(agg)) {
            return null;
        }

        if (agg instanceof InternalNumericMetricsAggregation.MultiValue) {
            //TODO: need to investigate when this can be not-null
            //if (innerKey == null) {
            //    throw new SqlIllegalArgumentException("Invalid innerKey {} specified for aggregation {}", innerKey, name);
            //}
            return handleDateTime(((InternalNumericMetricsAggregation.MultiValue) agg).value(property));
        } else if (agg instanceof InternalFilter) {
            // COUNT(expr) and COUNT(ALL expr) uses this type of aggregation to account for non-null values only
            return ((InternalFilter) agg).getDocCount();
        }

        Object v = agg.getProperty(property);
        return handleDateTime(innerKey != null && v instanceof Map ? ((Map<?, ?>) v).get(innerKey) : v);
    }

    private Object handleDateTime(Object object) {
        if (isDateTimeBased) {
            if (object == null) {
                return object;
            } else if (object instanceof Number) {
                return DateUtils.asDateTime(((Number) object).longValue(), zoneId);
            } else {
                throw new PlainIllegalArgumentException("Invalid date key returned: {}", object);
            }
        }
        return object;
    }

    /**
     * Check if the given aggregate has been executed and has computed values
     * or not (the bucket is null).
     *
     * Waiting on https://github.com/elastic/elasticsearch/issues/34903
     */
    private static boolean containsValues(InternalAggregation agg) {
        // Stats & ExtendedStats
        if (agg instanceof InternalStats) {
            return ((InternalStats) agg).getCount() != 0;
        }
//        if (agg instanceof MatrixStats) {
//            return ((MatrixStats) agg).getDocCount() != 0;
//        }
        // sum returns 0 even for null; since that's a common case, we return it as such
        if (agg instanceof SingleValue) {
            return Double.isFinite(((SingleValue) agg).value());
        }
        if (agg instanceof PercentileRanks) {
            return Double.isFinite(((PercentileRanks) agg).percent(0));
        }
        if (agg instanceof Percentiles) {
            return Double.isFinite(((Percentiles) agg).percentile(0));
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, property, innerKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MetricAggExtractor other = (MetricAggExtractor) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(property, other.property)
                && Objects.equals(innerKey, other.innerKey);
    }

    @Override
    public String toString() {
        String i = innerKey != null ? "[" + innerKey + "]" : "";
        return ROOT_GROUP_NAME + ">" + name + "." + property + i;
    }

}
