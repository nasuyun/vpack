package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.repository.RestoreSourceService;

import java.io.IOException;

public class ClearRestoreSessionAction extends Action<ClearRestoreSessionRequest,
        ClearRestoreSessionAction.ClearRestoreSessionResponse, ClearRestoreSessionRequestBuilder> {

    public static final ClearRestoreSessionAction INSTANCE = new ClearRestoreSessionAction();
    public static final String NAME = "internal:admin/xdcr/restore/session/clear";

    private ClearRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public ClearRestoreSessionResponse newResponse() {
        return new ClearRestoreSessionResponse();
    }

    @Override
    public ClearRestoreSessionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ClearRestoreSessionRequestBuilder(client);
    }

    public static class TransportDeleteRestoreSessionAction
        extends HandledTransportAction<ClearRestoreSessionRequest, ClearRestoreSessionResponse> {

        private final RestoreSourceService restoreService;

        @Inject
        public TransportDeleteRestoreSessionAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver resolver,
                                                   TransportService transportService, RestoreSourceService restoreService) {
            super(settings, NAME, threadPool, transportService, actionFilters, resolver, ClearRestoreSessionRequest::new,
                ThreadPool.Names.GENERIC);
            TransportActionProxy.registerProxyAction(transportService, NAME, ClearRestoreSessionResponse::new);
            this.restoreService = restoreService;
        }

        @Override
        protected void doExecute(ClearRestoreSessionRequest request, ActionListener<ClearRestoreSessionResponse> listener) {
            restoreService.closeSession(request.getSessionUUID());
            listener.onResponse(new ClearRestoreSessionResponse());
        }
    }

    public static class ClearRestoreSessionResponse extends ActionResponse {

        ClearRestoreSessionResponse() {
        }

        ClearRestoreSessionResponse(StreamInput in) throws IOException {
            super(in);
        }
    }
}
