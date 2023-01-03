package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.repository.RestoreSourceService;

import java.io.IOException;

public class GetRestoreFileChunkAction extends Action<GetRestoreFileChunkRequest,
        GetRestoreFileChunkAction.GetRestoreFileChunkResponse, GetRestoreFileChunkRequestBuilder> {

    public static final GetRestoreFileChunkAction INSTANCE = new GetRestoreFileChunkAction();
    public static final String NAME = "internal:admin/xdcr/restore/file_chunk/get";

    private GetRestoreFileChunkAction() {
        super(NAME);
    }

    @Override
    public GetRestoreFileChunkResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<GetRestoreFileChunkResponse> getResponseReader() {
        return GetRestoreFileChunkResponse::new;
    }

    @Override
    public GetRestoreFileChunkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetRestoreFileChunkRequestBuilder(client);
    }

    public static class TransportGetRestoreFileChunkAction
            extends HandledTransportAction<GetRestoreFileChunkRequest, GetRestoreFileChunkResponse> {

        private final RestoreSourceService restoreSourceService;
        private final BigArrays bigArrays;

        @Inject
        public TransportGetRestoreFileChunkAction(Settings settings, BigArrays bigArrays, TransportService transportService,
                                                  IndexNameExpressionResolver resolver,
                                                  ActionFilters actionFilters, RestoreSourceService restoreSourceService) {
            super(settings, NAME, transportService.getThreadPool(), transportService, actionFilters, resolver,
                    GetRestoreFileChunkRequest::new, ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, GetRestoreFileChunkResponse::new);
            this.restoreSourceService = restoreSourceService;
            this.bigArrays = bigArrays;
        }

        @Override
        protected void doExecute(GetRestoreFileChunkRequest request, ActionListener<GetRestoreFileChunkResponse> listener) {
            int bytesRequested = request.getSize();
            ByteArray array = bigArrays.newByteArray(bytesRequested, false);
            String fileName = request.getFileName();
            String sessionUUID = request.getSessionUUID();
            // This is currently safe to do because calling `onResponse` will serialize the bytes to the network layer data
            // structure on the same thread. So the bytes will be copied before the reference is released.
            try (ReleasablePagedBytesReference reference = new ReleasablePagedBytesReference(array, bytesRequested, array)) {
                try (RestoreSourceService.SessionReader sessionReader = restoreSourceService.getSessionReader(sessionUUID)) {
                    long offsetAfterRead = sessionReader.readFileBytes(fileName, reference);
                    long offsetBeforeRead = offsetAfterRead - reference.length();
                    listener.onResponse(new GetRestoreFileChunkResponse(offsetBeforeRead, reference));
                }
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }
    }

    public static class GetRestoreFileChunkResponse extends ActionResponse {

        private final long offset;
        private final BytesReference chunk;
        private final AbstractRefCounted ref = new AbstractRefCounted("response ref") {
            @Override
            protected void closeInternal() {
            }
        };

        GetRestoreFileChunkResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
            offset = streamInput.readVLong();
            chunk = streamInput.readBytesReference();
        }

        GetRestoreFileChunkResponse(long offset, BytesReference chunk) {
            this.offset = offset;
            this.chunk = chunk;
        }

        public long getOffset() {
            return offset;
        }

        public BytesReference getChunk() {
            return chunk;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(offset);
            out.writeBytesReference(chunk);
        }

        public void incRef() {
            ref.incRef();
        }

        public void decRef() {
            ref.decRef();
        }
    }
}
