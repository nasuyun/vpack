/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.action.repositories;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.repository.RestoreSourceService;

import java.io.IOException;

public class PutRestoreSessionAction extends Action<PutRestoreSessionRequest,
        PutRestoreSessionAction.PutRestoreSessionResponse, PutRestoreSessionRequestBuilder> {

    public static final PutRestoreSessionAction INSTANCE = new PutRestoreSessionAction();
    public static final String NAME = "internal:admin/xdcr/restore/session/put";

    private PutRestoreSessionAction() {
        super(NAME);
    }

    @Override
    public PutRestoreSessionResponse newResponse() {
        return new PutRestoreSessionResponse();
    }

    @Override
    public Writeable.Reader<PutRestoreSessionResponse> getResponseReader() {
        return PutRestoreSessionResponse::new;
    }

    @Override
    public PutRestoreSessionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PutRestoreSessionRequestBuilder(client);
    }

    public static class TransportPutRestoreSessionAction
        extends TransportSingleShardAction<PutRestoreSessionRequest, PutRestoreSessionResponse> {

        private final IndicesService indicesService;
        private final RestoreSourceService restoreService;

        @Inject
        public TransportPutRestoreSessionAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                                TransportService transportService, IndicesService indicesService,
                                                RestoreSourceService restoreService) {
            super(settings, NAME, threadPool, clusterService, transportService, actionFilters, resolver,
                PutRestoreSessionRequest::new, ThreadPool.Names.GENERIC);
            this.indicesService = indicesService;
            this.restoreService = restoreService;
        }

        @Override
        protected PutRestoreSessionResponse shardOperation(PutRestoreSessionRequest request, ShardId shardId) throws IOException {
            IndexShard indexShard = indicesService.getShardOrNull(shardId);
            if (indexShard == null) {
                throw new ShardNotFoundException(shardId);
            }
            Store.MetadataSnapshot storeFileMetaData = restoreService.openSession(request.getSessionUUID(), indexShard);
            long mappingVersion = indexShard.indexSettings().getIndexMetaData().getMappingVersion();
            return new PutRestoreSessionResponse(clusterService.localNode(), storeFileMetaData, mappingVersion);
        }

        @Override
        protected PutRestoreSessionResponse newResponse() {
            return new PutRestoreSessionResponse();
        }

        @Override
        protected boolean resolveIndex(PutRestoreSessionRequest request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            final ShardId shardId = request.request().getShardId();
            return state.routingTable().shardRoutingTable(shardId).primaryShardIt();
        }
    }


    public static class PutRestoreSessionResponse extends ActionResponse {

        private DiscoveryNode node;
        private Store.MetadataSnapshot storeFileMetaData;
        private long mappingVersion;

        PutRestoreSessionResponse() {
        }

        PutRestoreSessionResponse(DiscoveryNode node, Store.MetadataSnapshot storeFileMetaData, long mappingVersion) {
            this.node = node;
            this.storeFileMetaData = storeFileMetaData;
            this.mappingVersion = mappingVersion;
        }

        PutRestoreSessionResponse(StreamInput in) throws IOException {
            super(in);
            node = new DiscoveryNode(in);
            storeFileMetaData = new Store.MetadataSnapshot(in);
            mappingVersion = in.readVLong();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            node = new DiscoveryNode(in);
            storeFileMetaData = new Store.MetadataSnapshot(in);
            mappingVersion = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            node.writeTo(out);
            storeFileMetaData.writeTo(out);
            out.writeVLong(mappingVersion);
        }

        public DiscoveryNode getNode() {
            return node;
        }

        public Store.MetadataSnapshot getStoreFileMetaData() {
            return storeFileMetaData;
        }

        public long getMappingVersion() {
            return mappingVersion;
        }
    }
}
