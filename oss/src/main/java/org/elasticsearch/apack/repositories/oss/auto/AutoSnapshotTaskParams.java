package org.elasticsearch.apack.repositories.oss.auto;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 索引复制任务参数
 */
public class AutoSnapshotTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/oss/snapshot/auto";
    public static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueHours(1);
    public static final Integer DEFAULT_RETAIN = 5;

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<AutoSnapshotTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(NAME, (a) -> new AutoSnapshotTaskParams(
                    (String) a[0],
                    toInterval((String) a[1]),
                    (Integer) a[2],
                    toIndices((List<String>) a[3]))
            );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("repository"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("interval"));
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("retain"));
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), new ParseField("indices"));
    }

    static String[] toIndices(List<String> indices) {
        return indices == null || indices.isEmpty() ? new String[0] : indices.toArray(new String[indices.size()]);
    }

    static TimeValue toInterval(String interval) {
        return Strings.isNullOrEmpty(interval) ? DEFAULT_INTERVAL : TimeValue.parseTimeValue(interval, "");
    }

    private String repository;
    private TimeValue interval;
    private int retain;
    private String[] indices;

    public AutoSnapshotTaskParams(String repository, TimeValue interval, Integer retain, String[] indices) {
        this.repository = repository;
        this.interval = interval == null ? DEFAULT_INTERVAL : interval;
        this.retain = retain == null ? DEFAULT_RETAIN : retain;
        this.indices = indices == null ? new String[0] : indices;
    }

    public String repository() {
        return repository;
    }

    public String[] indices() {
        return indices;
    }

    public TimeValue interval() {
        return interval;
    }

    public int retain() {
        return retain;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    public AutoSnapshotTaskParams(StreamInput in) throws IOException {
        this.repository = in.readString();
        this.interval = TimeValue.timeValueMillis(in.readLong());
        this.retain = in.readInt();
        this.indices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeLong(interval.millis());
        out.writeInt(retain);
        out.writeStringArray(indices);
    }

    public static AutoSnapshotTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("interval", interval);
        builder.field("retain", retain);
        builder.array("indices", indices);
        builder.endObject();
        return builder;
    }

    public String toString() {
        return repository;
    }
}
