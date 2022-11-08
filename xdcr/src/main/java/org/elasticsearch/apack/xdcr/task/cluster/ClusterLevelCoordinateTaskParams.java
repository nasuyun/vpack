package org.elasticsearch.apack.xdcr.task.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.apack.xdcr.utils.Ssl.filteredHeaders;

/**
 * 索引复制任务参数
 */
public class ClusterLevelCoordinateTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/xdcr/sync/cluster";

    static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    static final ParseField INCLUDES_FIELD = new ParseField("includes");
    static final ParseField EXCLUDES_FIELD = new ParseField("excludes");
    private static final ParseField HEADERS = new ParseField("headers");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ClusterLevelCoordinateTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(NAME, (a) -> new ClusterLevelCoordinateTaskParams((String) a[0], (String) a[1], (String) a[2], (Map<String, String>) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INCLUDES_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), EXCLUDES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String repository;
    private final String includes;
    private final String excludes;
    private final Map<String, String> headers;

    public ClusterLevelCoordinateTaskParams(String repository, String includes, String excludes, Map<String, String> headers) {
        this.repository = repository;
        this.includes = includes;
        this.excludes = excludes;
        this.headers = filteredHeaders(headers);
    }

    public String repository() {
        return repository;
    }

    public String includes() {
        return includes;
    }

    public String excludes() {
        return excludes;
    }

    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    public ClusterLevelCoordinateTaskParams(StreamInput in) throws IOException {
        this.repository = in.readString();
        this.includes = in.readString();
        this.excludes = in.readString();
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(includes);
        out.writeString(excludes);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ClusterLevelCoordinateTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("includes", includes);
        builder.field("excludes", excludes);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.endObject();
        return builder;
    }

    public String toString() {
        return repository;
    }
}
