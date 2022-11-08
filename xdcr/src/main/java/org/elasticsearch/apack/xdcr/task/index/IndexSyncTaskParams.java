package org.elasticsearch.apack.xdcr.task.index;

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
public class IndexSyncTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/xdcr/sync/index";

    static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    static final ParseField INDEX_FIELD = new ParseField("index");
    static final ParseField INDEX_UUID_FIELD = new ParseField("indexUUID");
    private static final ParseField HEADERS = new ParseField("headers");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<IndexSyncTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(
                    NAME, (a) -> new IndexSyncTaskParams((String) a[0], (String) a[1], (String) a[2], (Map<String, String>) a[3])
            );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), HEADERS);
    }

    private final String repository;
    private final String index;
    private final String indexUUID;
    private final Map<String, String> headers;

    public IndexSyncTaskParams(String repository, String index, String indexUUID, Map<String, String> headers) {
        this.repository = repository;
        this.index = index;
        this.indexUUID = indexUUID;
        this.headers = filteredHeaders(headers);
    }

    public String repository() {
        return repository;
    }

    public String index() {
        return index;
    }

    public String indexUUID() {
        return indexUUID;
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

    public IndexSyncTaskParams(StreamInput in) throws IOException {
        this.repository = in.readString();
        this.index = in.readString();
        this.indexUUID = in.readString();
        this.headers = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(index);
        out.writeString(indexUUID);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static IndexSyncTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("index", index);
        builder.field("indexUUID", indexUUID);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.endObject();
        return builder;
    }

    public String toString() {
        return repository + ":" + index + ":" + indexUUID;
    }
}
