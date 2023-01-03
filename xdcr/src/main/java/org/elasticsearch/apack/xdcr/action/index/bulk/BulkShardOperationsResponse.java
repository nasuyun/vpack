package org.elasticsearch.apack.xdcr.action.index.bulk;

import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class BulkShardOperationsResponse extends ReplicationResponse implements WriteResponse {

    private long globalCheckpoint;

    public long getGlobalCheckpoint() {
        return globalCheckpoint;
    }

    public void setGlobalCheckpoint(final long globalCheckpoint) {
        this.globalCheckpoint = globalCheckpoint;
    }

    private long maxSeqNo;

    public long getMaxSeqNo() {
        return maxSeqNo;
    }

    public void setMaxSeqNo(final long maxSeqNo) {
        this.maxSeqNo = maxSeqNo;
    }

    public BulkShardOperationsResponse() {
    }

    @Override
    public void setForcedRefresh(final boolean forcedRefresh) {
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        globalCheckpoint = in.readZLong();
        maxSeqNo = in.readZLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(globalCheckpoint);
        out.writeZLong(maxSeqNo);
    }

}
