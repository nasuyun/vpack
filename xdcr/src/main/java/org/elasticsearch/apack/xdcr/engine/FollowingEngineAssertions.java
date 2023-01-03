package org.elasticsearch.apack.xdcr.engine;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;


final class FollowingEngineAssertions {

    static boolean preFlight(final Engine.Operation operation) {
        assert operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
        return true;
    }

    static boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations on a following index must have an assigned sequence number";
        return true;
    }

}
