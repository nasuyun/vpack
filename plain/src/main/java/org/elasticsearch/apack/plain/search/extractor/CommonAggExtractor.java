package org.elasticsearch.apack.plain.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.io.IOException;
import java.time.ZoneId;

public class CommonAggExtractor implements AggsExtractor<Aggregations> {

    static final String NAME = "a";

    private final String fieldName;
    private final ZoneId zoneId;

    public CommonAggExtractor(String fieldName, ZoneId zoneId) {
        this.fieldName = fieldName;
        this.zoneId = zoneId;
    }

    @Override
    public Object extract(Aggregations response) {
        Aggregation agg = response.getAsMap().get(fieldName);
        if (agg instanceof MultiBucketsAggregation) {
            return buketsValue((MultiBucketsAggregation) agg);
        }
        if (agg instanceof NumericMetricsAggregation.SingleValue) {
            return ((NumericMetricsAggregation.SingleValue) agg).value();
        }
        return response;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private String buketsValue(MultiBucketsAggregation buckets) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < buckets.getBuckets().size(); i++) {
            Bucket bucket = buckets.getBuckets().get(i);
            buffer.append("count(" + bucket.getKey() + ")=" + bucket.getDocCount());
            if (i < (buckets.getBuckets().size() - 1)) {
                buffer.append(",");
            }
        }
        return buffer.toString();
    }

    CommonAggExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        zoneId = ZoneId.of(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(zoneId.getId());
    }
}
