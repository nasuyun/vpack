package org.elasticsearch.apack.xdcr.repository;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public abstract class MultiChunkTransfer<Source, Request extends MultiChunkTransfer.ChunkRequest> implements Closeable {
    private Status status = Status.PROCESSING;
    private final Logger logger;
    private final ActionListener<Void> listener;
    private final LocalCheckpointTracker requestSeqIdTracker = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final AsyncIOProcessor<FileChunkResponseItem<Source>> processor;
    private final int maxConcurrentChunks;
    private Source currentSource = null;
    private final Iterator<Source> remainingSources;
    private Tuple<Source, Request> readAheadRequest = null;


    protected MultiChunkTransfer(
            Logger logger,
            ThreadContext threadContext,
            ActionListener<Void> listener,
            int maxConcurrentChunks,
            List<Source> sources
    ) {
        this.logger = logger;
        this.maxConcurrentChunks = maxConcurrentChunks;
        this.listener = listener;
        this.processor = new AsyncIOProcessor<FileChunkResponseItem<Source>>(logger, maxConcurrentChunks, threadContext) {
            @Override
            protected void write(List<Tuple<FileChunkResponseItem<Source>, Consumer<Exception>>> items) throws IOException {
                handleItems(items);
            }
        };
        this.remainingSources = sources.iterator();
    }

    public final void start() {
        addItem(UNASSIGNED_SEQ_NO, null, null); // put a dummy item to start the processor
    }

    private void addItem(long requestSeqId, Source resource, Exception failure) {
        processor.put(new FileChunkResponseItem<>(requestSeqId, resource, failure), e -> {
            assert e == null : e;
        });
    }

    private void handleItems(List<Tuple<FileChunkResponseItem<Source>, Consumer<Exception>>> items) {
        if (status != Status.PROCESSING) {
            assert status == Status.FAILED : "must not receive any response after the transfer was completed";
            // These exceptions will be ignored as we record only the first failure, log them for debugging purpose.
            items.stream()
                    .filter(item -> item.v1().failure != null)
                    .forEach(
                            item -> logger.debug(
                                    new ParameterizedMessage("failed to transfer a chunk request {}", item.v1().source),
                                    item.v1().failure
                            )
                    );
            return;
        }
        try {
            for (Tuple<FileChunkResponseItem<Source>, Consumer<Exception>> item : items) {
                final FileChunkResponseItem<Source> resp = item.v1();
                if (resp.requestSeqId == UNASSIGNED_SEQ_NO) {
                    continue; // not an actual item
                }
                requestSeqIdTracker.markSeqNoAsCompleted(resp.requestSeqId);
                if (resp.failure != null) {
                    handleError(resp.source, resp.failure);
                    throw resp.failure;
                }
            }
            while (requestSeqIdTracker.getMaxSeqNo() - requestSeqIdTracker.getCheckpoint() < maxConcurrentChunks) {
                final Tuple<Source, Request> request = readAheadRequest != null ? readAheadRequest : getNextRequest();
                readAheadRequest = null;
                if (request == null) {
                    assert currentSource == null && remainingSources.hasNext() == false;
                    if (requestSeqIdTracker.getMaxSeqNo() == requestSeqIdTracker.getCheckpoint()) {
                        onCompleted(null);
                    }
                    return;
                }
                final long requestSeqId = requestSeqIdTracker.generateSeqNo();
                executeChunkRequest(
                        request.v2(),
                        ActionListener.wrap(r -> addItem(requestSeqId, request.v1(), null), e -> addItem(requestSeqId, request.v1(), e))
                );
            }
            // While we are waiting for the responses, we can prepare the next request in advance
            // so we can send it immediately when the responses arrive to reduce the transfer time.
            if (readAheadRequest == null) {
                readAheadRequest = getNextRequest();
            }
        } catch (Exception e) {
            onCompleted(e);
        }
    }

    protected boolean assertOnSuccess() {
        return true;
    }

    private void onCompleted(Exception failure) {
        if (Assertions.ENABLED && status != Status.PROCESSING) {
            throw new AssertionError("invalid status: expected [" + Status.PROCESSING + "] actual [" + status + "]", failure);
        }
        status = failure == null ? Status.SUCCESS : Status.FAILED;
        assert status != Status.SUCCESS || assertOnSuccess();
        try {
            IOUtils.close(failure, this);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    private Tuple<Source, Request> getNextRequest() throws Exception {
        try {
            if (currentSource == null) {
                if (remainingSources.hasNext()) {
                    currentSource = remainingSources.next();
                    onNewResource(currentSource);
                } else {
                    return null;
                }
            }
            final Source md = currentSource;
            final Request request = nextChunkRequest(md);
            if (request.lastChunk()) {
                currentSource = null;
            }
            return Tuple.tuple(md, request);
        } catch (Exception e) {
            handleError(currentSource, e);
            throw e;
        }
    }

    /**
     * This method is called when starting sending/requesting a new source. Subclasses should override
     * this method to reset the file offset or close the previous file and open a new file if needed.
     *
     * @param resource source
     * @throws IOException Exception
     */
    protected void onNewResource(Source resource) throws IOException {

    }

    protected abstract Request nextChunkRequest(Source resource) throws IOException;

    protected abstract void executeChunkRequest(Request request, ActionListener<Void> listener);

    protected abstract void handleError(Source resource, Exception e) throws Exception;

    private static class FileChunkResponseItem<Source> {
        final long requestSeqId;
        final Source source;
        final Exception failure;

        FileChunkResponseItem(long requestSeqId, Source source, Exception failure) {
            this.requestSeqId = requestSeqId;
            this.source = source;
            this.failure = failure;
        }
    }

    public interface ChunkRequest {
        /**
         * @return {@code true} if this chunk request is the last chunk of the current file
         */
        boolean lastChunk();
    }

    private enum Status {
        PROCESSING,
        SUCCESS,
        FAILED
    }
}
