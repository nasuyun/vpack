package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class PutInternalRepositoryAction extends GenericAction<PutInternalRepositoryRequest,
        PutInternalRepositoryAction.PutInternalRepositoryResponse> {

    public static final PutInternalRepositoryAction INSTANCE = new PutInternalRepositoryAction();
    public static final String NAME = "internal:admin/xdcr/internal_repository/put";

    private PutInternalRepositoryAction() {
        super(NAME);
    }

    @Override
    public PutInternalRepositoryResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<PutInternalRepositoryResponse> getResponseReader() {
        return PutInternalRepositoryResponse::new;
    }

    public static class TransportPutInternalRepositoryAction
        extends TransportAction<PutInternalRepositoryRequest, PutInternalRepositoryResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportPutInternalRepositoryAction(Settings settings, ThreadPool threadPool, RepositoriesService repositoriesService,
                                                    ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                    TransportService transportService) {
            super(settings, NAME, threadPool, actionFilters, resolver, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(PutInternalRepositoryRequest request, ActionListener<PutInternalRepositoryResponse> listener) {
            repositoriesService.registerInternalRepository(request.getName(), request.getType());
            listener.onResponse(new PutInternalRepositoryResponse());

        }
    }

    public static class PutInternalRepositoryResponse extends ActionResponse {

        PutInternalRepositoryResponse() {
            super();
        }

        PutInternalRepositoryResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
        }
    }
}
