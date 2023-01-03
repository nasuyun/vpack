package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class PutRestoreSessionRequest extends SingleShardRequest<PutRestoreSessionRequest> {

    private String sessionUUID;
    private ShardId shardId;

    PutRestoreSessionRequest() {
    }

    public PutRestoreSessionRequest(String sessionUUID, ShardId shardId) {
        super(shardId.getIndexName());
        this.sessionUUID = sessionUUID;
        this.shardId = shardId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sessionUUID = in.readString();
        shardId = ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionUUID);
        shardId.writeTo(out);
    }

    String getSessionUUID() {
        return sessionUUID;
    }

    ShardId getShardId() {
        return shardId;
    }
}
