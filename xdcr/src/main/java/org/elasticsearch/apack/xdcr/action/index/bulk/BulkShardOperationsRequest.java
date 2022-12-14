package org.elasticsearch.apack.xdcr.action.index.bulk;

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

public final class BulkShardOperationsRequest extends ReplicatedWriteRequest<BulkShardOperationsRequest> {

    private List<Translog.Operation> operations;
    private long maxSeqNoOfUpdatesOrDeletes;

    public BulkShardOperationsRequest() {
    }

    public BulkShardOperationsRequest(final ShardId shardId,
                                      final List<Translog.Operation> operations,
                                      long maxSeqNoOfUpdatesOrDeletes) {
        super(shardId);
        setRefreshPolicy(RefreshPolicy.NONE);
        this.operations = operations;
        this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
    }

    public List<Translog.Operation> getOperations() {
        return operations;
    }

    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return maxSeqNoOfUpdatesOrDeletes;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        maxSeqNoOfUpdatesOrDeletes = in.readZLong();
        operations = in.readList(Translog.Operation::readOperation);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
        out.writeVInt(operations.size());
        for (Translog.Operation operation : operations) {
            Translog.Operation.writeOperation(out, operation);
        }
    }

    @Override
    public String toString() {
        return "BulkShardOperationsRequest{" +
                ", operations=" + operations.size() +
                ", maxSeqNoUpdates=" + maxSeqNoOfUpdatesOrDeletes +
                ", shardId=" + shardId +
                ", timeout=" + timeout +
                ", index='" + index + '\'' +
                ", waitForActiveShards=" + waitForActiveShards +
                '}';
    }

}
