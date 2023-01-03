package org.elasticsearch.apack.xdcr.engine;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;

import java.util.OptionalLong;

public final class AlreadyProcessedFollowingEngineException extends VersionConflictEngineException {
    private final long seqNo;
    private final OptionalLong existingPrimaryTerm;

    AlreadyProcessedFollowingEngineException(ShardId shardId, long seqNo, OptionalLong existingPrimaryTerm) {
        super(shardId, "operation [{}] was processed before with term [{}]", null, seqNo, existingPrimaryTerm);
        this.seqNo = seqNo;
        this.existingPrimaryTerm = existingPrimaryTerm;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public OptionalLong getExistingPrimaryTerm() {
        return existingPrimaryTerm;
    }
}
