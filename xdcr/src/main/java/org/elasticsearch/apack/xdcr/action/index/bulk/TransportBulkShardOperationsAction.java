/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.action.index.bulk;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.engine.AlreadyProcessedFollowingEngineException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportBulkShardOperationsAction
        extends TransportWriteAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

    @Inject
    public TransportBulkShardOperationsAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
                settings,
                BulkShardOperationsAction.NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                indexNameExpressionResolver,
                BulkShardOperationsRequest::new,
                BulkShardOperationsRequest::new,
                ThreadPool.Names.WRITE);
    }

    @Override
    protected WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> shardOperationOnPrimary(
            final BulkShardOperationsRequest request, final IndexShard primary) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("index [{}] on the following primary shard {}", request.getOperations(), primary.routingEntry());
        }
        return shardOperationOnPrimary(request.shardId(), request.getOperations(),
                request.getMaxSeqNoOfUpdatesOrDeletes(), primary, logger);
    }

    static Translog.Operation rewriteOperationWithPrimaryTerm(Translog.Operation operation, long primaryTerm) {
        final Translog.Operation operationWithPrimaryTerm;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                operationWithPrimaryTerm = new Translog.Index(
                        index.type(),
                        index.id(),
                        index.seqNo(),
                        primaryTerm,
                        index.version(),
                        index.versionType(),
                        BytesReference.toBytes(index.source()),
                        index.routing(),
                        index.parent(),
                        index.getAutoGeneratedIdTimestamp());
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                operationWithPrimaryTerm = new Translog.Delete(
                        delete.type(),
                        delete.id(),
                        delete.uid(),
                        delete.seqNo(),
                        primaryTerm,
                        delete.version(),
                        delete.versionType());
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                operationWithPrimaryTerm = new Translog.NoOp(noOp.seqNo(), primaryTerm, noOp.reason());
                break;
            default:
                throw new IllegalStateException("unexpected operation type [" + operation.opType() + "]");
        }
        return operationWithPrimaryTerm;
    }

    // public for testing purposes only
    public static OnceWritePrimaryResult shardOperationOnPrimary(
            final ShardId shardId,
            final List<Translog.Operation> sourceOperations,
            final long maxSeqNoOfUpdatesOrDeletes,
            final IndexShard primary,
            final Logger logger) throws IOException {

        assert maxSeqNoOfUpdatesOrDeletes >= SequenceNumbers.NO_OPS_PERFORMED : "invalid msu [" + maxSeqNoOfUpdatesOrDeletes + "]";
        primary.advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);

        final List<Translog.Operation> appliedOperations = new ArrayList<>(sourceOperations.size());
        Translog.Location location = null;
        for (Translog.Operation sourceOp : sourceOperations) {
            if (primary.getLocalCheckpoint() + 1 != sourceOp.seqNo()) {
                throw new IllegalStateException(shardId + " write primary error, invalid seqno expect[" + (primary.getLocalCheckpoint() + 1) + "] received[" + sourceOp.seqNo() + "]");
            }
            final Translog.Operation targetOp = rewriteOperationWithPrimaryTerm(sourceOp, primary.getOperationPrimaryTerm());
            final Engine.Result result = primary.applyTranslogOperation(targetOp, Engine.Operation.Origin.PRIMARY);
            if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                assert result.getSeqNo() == targetOp.seqNo();
                appliedOperations.add(targetOp);
                location = locationToSync(location, result.getTranslogLocation());
            } else {
                if (result.getFailure() instanceof AlreadyProcessedFollowingEngineException) {
                    final AlreadyProcessedFollowingEngineException failure = (AlreadyProcessedFollowingEngineException) result.getFailure();
                    if (logger.isTraceEnabled()) {
                        logger.trace("operation [{}] was processed before on following primary shard {} with existing term {}",
                                targetOp, primary.routingEntry(), failure.getExistingPrimaryTerm());
                    }
                    assert failure.getSeqNo() == targetOp.seqNo() : targetOp.seqNo() + " != " + failure.getSeqNo();
                    if (failure.getExistingPrimaryTerm().isPresent()) {
                        appliedOperations.add(rewriteOperationWithPrimaryTerm(sourceOp, failure.getExistingPrimaryTerm().getAsLong()));
                    } else if (targetOp.seqNo() > primary.getGlobalCheckpoint()) {
                        assert false : "can't find primary_term for existing op=" + targetOp + " gcp=" + primary.getGlobalCheckpoint();
                        throw new IllegalStateException("can't find primary_term for existing op=" + targetOp +
                                " global_checkpoint=" + primary.getGlobalCheckpoint(), failure);
                    }
                } else {
                    assert false : "Only already-processed error should happen; op=[" + targetOp + "] error=[" + result.getFailure() + "]";
                    throw ExceptionsHelper.convertToElastic(result.getFailure());
                }
            }
        }
        final BulkShardOperationsRequest replicaRequest = new BulkShardOperationsRequest(
                shardId, appliedOperations, maxSeqNoOfUpdatesOrDeletes);
        return new OnceWritePrimaryResult(replicaRequest, location, primary, logger);
    }

    @Override
    protected WriteReplicaResult<BulkShardOperationsRequest> shardOperationOnReplica(
            final BulkShardOperationsRequest request, final IndexShard replica) throws Exception {
        if (logger.isTraceEnabled()) {
            logger.trace("index [{}] on the following replica shard {}", request.getOperations(), replica.routingEntry());
        }
        return shardOperationOnReplica(request, replica, logger);
    }

    public static WriteReplicaResult<BulkShardOperationsRequest> shardOperationOnReplica(
            final BulkShardOperationsRequest request, final IndexShard replica, final Logger logger) throws IOException {
        assert replica.getMaxSeqNoOfUpdatesOrDeletes() >= request.getMaxSeqNoOfUpdatesOrDeletes() :
                "mus on replica [" + replica + "] < mus notNull request [" + request.getMaxSeqNoOfUpdatesOrDeletes() + "]";
        Translog.Location location = null;
        for (final Translog.Operation operation : request.getOperations()) {
            final Engine.Result result = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA);
            if (result.getResultType() != Engine.Result.Type.SUCCESS) {
                assert false : "doc-level failure must not happen on replicas; op[" + operation + "] error[" + result.getFailure() + "]";
                throw ExceptionsHelper.convertToElastic(result.getFailure());
            }
            assert result.getSeqNo() == operation.seqNo();
            location = locationToSync(location, result.getTranslogLocation());
        }
        assert request.getOperations().size() == 0 || location != null;
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    @Override
    protected BulkShardOperationsResponse newResponseInstance() {
        return new BulkShardOperationsResponse();
    }

    static final class OnceWritePrimaryResult extends WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> {
        OnceWritePrimaryResult(BulkShardOperationsRequest request, Translog.Location location, IndexShard primary, Logger logger) {
            super(request, new BulkShardOperationsResponse(), location, null, primary, logger);
        }

        @Override
        public synchronized void respond(ActionListener<BulkShardOperationsResponse> listener) {
            final ActionListener<BulkShardOperationsResponse> wrappedListener = ActionListener.wrap(response -> {
                final SeqNoStats seqNoStats = primary.seqNoStats();
                response.setGlobalCheckpoint(seqNoStats.getGlobalCheckpoint());
                response.setMaxSeqNo(seqNoStats.getMaxSeqNo());
                listener.onResponse(response);
            }, listener::onFailure);
            super.respond(wrappedListener);
        }

    }


}
