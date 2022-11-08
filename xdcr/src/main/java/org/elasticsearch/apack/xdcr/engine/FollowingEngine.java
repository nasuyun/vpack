/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.apack.XDCRSettings;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;


public final class FollowingEngine extends InternalEngine {

    private final CounterMetric numOfOptimizedIndexing = new CounterMetric();

    FollowingEngine(final EngineConfig engineConfig) {
        super(validateEngineConfig(engineConfig));
    }

    private static EngineConfig validateEngineConfig(final EngineConfig engineConfig) {
        if (XDCRSettings.XDCR_FOLLOWING_INDEX_SETTING.get(engineConfig.getIndexSettings().getSettings()) == false) {
            throw new IllegalArgumentException("a following engine can not be constructed for a non-following index");
        }
        // 直接拷贝一阶段索引则不需要校验soft_delete_enabled
        // if (engineConfig.getIndexSettings().isSoftDeleteEnabled() == false) {
        //    throw new IllegalArgumentException("a following engine requires soft deletes to be enabled");
        // }
        return engineConfig;
    }

    private void preFlight(final Operation operation) {
        assert FollowingEngineAssertions.preFlight(operation);
        if (operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new ElasticsearchStatusException("a following engine does not accept operations without an assigned sequence number",
                    RestStatus.FORBIDDEN);
        }
    }

    @Override
    protected InternalEngine.IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        preFlight(index);
        markSeqNoAsSeen(index.seqNo());
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        if (hasBeenProcessedBefore(index)) {
            if (logger.isTraceEnabled()) {
                logger.trace("index operation [id={} seq_no={} origin={}] was processed before", index.id(), index.seqNo(), index.origin());
            }
            if (index.origin() == Operation.Origin.PRIMARY) {
                final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(
                        shardId, index.seqNo(), lookupPrimaryTerm(index.seqNo()));
                return IndexingStrategy.skipDueToVersionConflict(error, false, index.version(), index.primaryTerm());
            } else {
                return IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            }
        } else if (maxSeqNoOfUpdatesOrDeletes <= getLocalCheckpoint()) {
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : "seq_no[" + index.seqNo() + "] <= msu[" + maxSeqNoOfUpdatesOrDeletes + "]";
            numOfOptimizedIndexing.inc();
            return InternalEngine.IndexingStrategy.optimizedAppendOnly(index.seqNo(), index.version());

        } else {
            return planIndexingAsNonPrimary(index);
        }
    }

    @Override
    protected InternalEngine.DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        preFlight(delete);
        markSeqNoAsSeen(delete.seqNo());
        if (delete.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(delete)) {
            final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(
                    shardId, delete.seqNo(), lookupPrimaryTerm(delete.seqNo()));
            return DeletionStrategy.skipDueToVersionConflict(error, delete.version(), delete.primaryTerm(), false);
        } else {
            return planDeletionAsNonPrimary(delete);
        }
    }

    @Override
    protected Optional<Exception> preFlightCheckForNoOp(NoOp noOp) throws IOException {
        if (noOp.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(noOp)) {
            final OptionalLong existingTerm = lookupPrimaryTerm(noOp.seqNo());
            return Optional.of(new AlreadyProcessedFollowingEngineException(shardId, noOp.seqNo(), existingTerm));
        } else {
            return super.preFlightCheckForNoOp(noOp);
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    protected boolean assertPrimaryIncomingSequenceNumber(final Operation.Origin origin, final long seqNo) {
        assert FollowingEngineAssertions.assertPrimaryIncomingSequenceNumber(origin, seqNo);
        return true;
    }

    @Override
    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        return true;
    }

    @Override
    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL
                : "version [" + index.version() + "], type [" + index.versionType() + "]";
        return true;
    }

    private OptionalLong lookupPrimaryTerm(final long seqNo) throws IOException {
        if (seqNo <= engineConfig.getGlobalCheckpointSupplier().getAsLong()) {
            return OptionalLong.empty();
        }
        refreshIfNeeded("lookup_primary_term", seqNo);
        try (Searcher engineSearcher = acquireSearcher("lookup_primary_term", SearcherScope.INTERNAL)) {
            final DirectoryReader reader = Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader());
            final IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setQueryCache(null);
            final Query query = new BooleanQuery.Builder()
                    .add(LongPoint.newExactQuery(SeqNoFieldMapper.NAME, seqNo), BooleanClause.Occur.FILTER)
                    .add(new DocValuesFieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME), BooleanClause.Occur.FILTER)
                    .build();
            final TopDocs topDocs = searcher.search(query, 1);
            if (topDocs.scoreDocs.length == 1) {
                final int docId = topDocs.scoreDocs[0].doc;
                final LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(docId, reader.leaves()));
                final NumericDocValues primaryTermDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                if (primaryTermDV != null && primaryTermDV.advanceExact(docId - leaf.docBase)) {
                    assert primaryTermDV.longValue() > 0 : "invalid term [" + primaryTermDV.longValue() + "]";
                    return OptionalLong.of(primaryTermDV.longValue());
                }
            }
            if (seqNo <= engineConfig.getGlobalCheckpointSupplier().getAsLong()) {
                return OptionalLong.empty();
            } else {
                assert false : "seq_no[" + seqNo + "] does not have primary_term, total_hits=[" + topDocs.totalHits + "]";
                throw new IllegalStateException("seq_no[" + seqNo + "] does not have primary_term (total_hits=" + topDocs.totalHits + ")");
            }
        } catch (IOException e) {
            try {
                maybeFailEngine("lookup_primary_term", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    public long getNumberOfOptimizedIndexing() {
        return numOfOptimizedIndexing.count();
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
    }
}
