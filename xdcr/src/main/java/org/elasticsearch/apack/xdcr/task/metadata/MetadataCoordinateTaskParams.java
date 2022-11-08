package org.elasticsearch.apack.xdcr.task.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;

public class MetadataCoordinateTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/xdcr/sync/metadata";

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<MetadataCoordinateTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(NAME, (a) -> new MetadataCoordinateTaskParams());

    public MetadataCoordinateTaskParams() {

    }

    public MetadataCoordinateTaskParams(StreamInput in) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public static MetadataCoordinateTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }
}
