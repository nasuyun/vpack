/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.apack;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.apack.xdcr.action.repositories.DeleteInternalRepositoryAction;
import org.elasticsearch.apack.xdcr.action.repositories.DeleteInternalRepositoryRequest;
import org.elasticsearch.apack.xdcr.action.repositories.PutInternalRepositoryAction;
import org.elasticsearch.apack.xdcr.action.repositories.PutInternalRepositoryRequest;
import org.elasticsearch.apack.xdcr.repository.RemoteRepository;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionStrategy;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.apack.xdcr.repository.RemoteRepository.NAME_PREFIX;

class XDCRRepositoryManager extends AbstractLifecycleComponent {

    private final NodeClient client;
    private final RemoteSettingsUpdateListener updateListener;

    XDCRRepositoryManager(Settings settings, ClusterService clusterService, NodeClient client) {
        this.client = client;
        updateListener = new RemoteSettingsUpdateListener(settings);
        updateListener.listenForUpdates(clusterService.getClusterSettings());
    }

    @Override
    protected void doStart() {
        updateListener.init();
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
    }

    private void putRepository(String repositoryName) {
        PutInternalRepositoryRequest request = new PutInternalRepositoryRequest(repositoryName, RemoteRepository.TYPE);
        PlainActionFuture<PutInternalRepositoryAction.PutInternalRepositoryResponse> f = PlainActionFuture.newFuture();
        client.executeLocally(PutInternalRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private void deleteRepository(String repositoryName) {
        DeleteInternalRepositoryRequest request = new DeleteInternalRepositoryRequest(repositoryName);
        PlainActionFuture<DeleteInternalRepositoryAction.DeleteInternalRepositoryResponse> f = PlainActionFuture.newFuture();
        client.executeLocally(DeleteInternalRepositoryAction.INSTANCE, request, f);
        assert f.isDone() : "Should be completed as it is executed synchronously";
    }

    private class RemoteSettingsUpdateListener extends RemoteClusterAware {

        private RemoteSettingsUpdateListener(Settings settings) {
            super(settings);
        }

        void init() {
            Set<String> clusterAliases = getEnabledRemoteClusters(settings);
            for (String clusterAlias : clusterAliases) {
                putRepository(NAME_PREFIX + clusterAlias);
            }
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, Settings settings) {
            String repositoryName = NAME_PREFIX + clusterAlias;
            if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
                putRepository(repositoryName);
            } else {
                deleteRepository(repositoryName);
            }
        }
    }
}
