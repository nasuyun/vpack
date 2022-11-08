package org.elasticsearch.apack.xdcr.action.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;

import java.io.IOException;

public class Stats implements Writeable, ToXContentFragment {

    public String index;
    public String repository;
    public int shard;
    public SeqNoStats localSeqNoStats;
    public SeqNoStats remoteSeqNoStats;

    public Stats(String repository, String index, int shard, SeqNoStats localSeqNoStats, SeqNoStats remoteSeqNoStats) {
        this.repository = repository;
        this.index = index;
        this.shard = shard;
        this.localSeqNoStats = localSeqNoStats;
        this.remoteSeqNoStats = remoteSeqNoStats;
    }

    public Stats(StreamInput in) throws IOException {
        repository = in.readString();
        index = in.readString();
        shard = in.readInt();
        localSeqNoStats = new SeqNoStats(in);
        remoteSeqNoStats = new SeqNoStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(index);
        out.writeInt(shard);
        localSeqNoStats.writeTo(out);
        remoteSeqNoStats.writeTo(out);
    }

    public static Stats readStats(StreamInput in) throws IOException {
        return new Stats(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("index", index);
        builder.field("shard", shard);
        builder.startObject("local");
        localSeqNoStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("remote");
        remoteSeqNoStats.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}