/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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

public class DeleteInternalRepositoryAction extends GenericAction<DeleteInternalRepositoryRequest,
        DeleteInternalRepositoryAction.DeleteInternalRepositoryResponse> {

    public static final DeleteInternalRepositoryAction INSTANCE = new DeleteInternalRepositoryAction();
    public static final String NAME = "internal:admin/xdcr/internal_repository/delete";

    private DeleteInternalRepositoryAction() {
        super(NAME);
    }

    @Override
    public DeleteInternalRepositoryResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<DeleteInternalRepositoryResponse> getResponseReader() {
        return DeleteInternalRepositoryResponse::new;
    }

    public static class TransportDeleteInternalRepositoryAction
        extends TransportAction<DeleteInternalRepositoryRequest, DeleteInternalRepositoryResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportDeleteInternalRepositoryAction(Settings settings, ThreadPool threadPool, RepositoriesService repositoriesService,
                                                       ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                       TransportService transportService) {
            super(settings, NAME, threadPool, actionFilters, resolver, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(DeleteInternalRepositoryRequest request,
                                 ActionListener<DeleteInternalRepositoryResponse> listener) {
            repositoriesService.unregisterInternalRepository(request.getName());
            listener.onResponse(new DeleteInternalRepositoryResponse());
        }
    }

    public static class DeleteInternalRepositoryResponse extends ActionResponse {

        DeleteInternalRepositoryResponse() {
            super();
        }

        DeleteInternalRepositoryResponse(StreamInput streamInput) throws IOException {
            super(streamInput);
        }
    }
}
