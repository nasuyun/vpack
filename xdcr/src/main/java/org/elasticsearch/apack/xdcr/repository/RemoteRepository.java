package org.elasticsearch.apack.xdcr.repository;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.*;
import org.elasticsearch.action.support.ListenerTimeouts;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.apack.RetentionLeases;
import org.elasticsearch.apack.xdcr.action.repositories.*;
import org.elasticsearch.apack.xdcr.utils.Requests;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRecoveryException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreException;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.FileRestoreContext;
import org.elasticsearch.snapshots.*;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.apack.RetentionLeases.*;
import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;


public class RemoteRepository extends AbstractLifecycleComponent implements Repository {

    private static final Logger logger = LogManager.getLogger(RemoteRepository.class);

    public static final String LATEST = "_latest_";
    public static final String TYPE = "_xdcr_";
    public static final String NAME_PREFIX = "_xdcr_";
    private static final SnapshotId SNAPSHOT_ID = new SnapshotId(LATEST, LATEST);
    private static final String IN_SYNC_ALLOCATION_ID = "xdcr_restore";

    private final RepositoryMetaData metadata;
    private final org.elasticsearch.apack.XDCRSettings XDCRSettings;
    private final String localClusterName;
    private final String remoteClusterAlias;
    private final Client client;
    private final ThreadPool threadPool;

    private final CounterMetric throttledTime = new CounterMetric();

    public RemoteRepository(RepositoryMetaData metadata, Client client, Settings settings,
                            org.elasticsearch.apack.XDCRSettings XDCRSettings, ThreadPool threadPool) {
        this.metadata = metadata;
        this.XDCRSettings = XDCRSettings;
        this.localClusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        assert metadata.name().startsWith(NAME_PREFIX) : "RemoteRepository metadata.name() must start with: " + NAME_PREFIX;
        this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

    @Override
    public RepositoryMetaData getMetadata() {
        return metadata;
    }

    private Client getRemoteClusterClient() {
        return client.getRemoteClusterClient(remoteClusterAlias);
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = getRemoteClusterClient();
        ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true).setNodes(true)
                .get(XDCRSettings.getRecoveryActionTimeout());
        ImmutableOpenMap<String, IndexMetaData> indicesMap = response.getState().metaData().indices();
        ArrayList<String> indices = new ArrayList<>(indicesMap.size());
        indicesMap.keysIt().forEachRemaining(indices::add);

        return new SnapshotInfo(snapshotId, indices, SnapshotState.SUCCESS, response.getState().getNodes().getMaxNodeVersion());
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        Client remoteClient = getRemoteClusterClient();
        // We set a single dummy index name to avoid fetching all the index data
        ClusterStateRequest clusterStateRequest = Requests.metaDataRequest("dummy_index_name");
        ClusterStateResponse clusterState = remoteClient.admin().cluster().state(clusterStateRequest)
                .actionGet(XDCRSettings.getRecoveryActionTimeout());
        return clusterState.getState().metaData();
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        assert SNAPSHOT_ID.equals(snapshotId) : "RemoteClusterRepository only supports " + SNAPSHOT_ID + " as the SnapshotId";
        String leaderIndex = index.getName();
        Client remoteClient = client.getRemoteClusterClient(remoteClusterAlias);

        ClusterStateResponse response = remoteClient
                .admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetaData(true)
                .setIndices(leaderIndex)
                .get();

        // Validates whether the leader cluster has been configured properly:
        PlainActionFuture<String[]> future = PlainActionFuture.newFuture();
        IndexMetaData leaderIndexMetaData = response.getState().metaData().index(leaderIndex);
        fetchLeaderHistoryUUIDs(remoteClient, leaderIndexMetaData, future::onFailure, future::onResponse);
        String[] leaderHistoryUUIDs = future.actionGet();

        IndexMetaData.Builder imdBuilder = IndexMetaData.builder(leaderIndexMetaData);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", leaderHistoryUUIDs));
        metadata.put(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING.getKey(), leaderIndexMetaData.getIndexUUID());
        metadata.put(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_NAME_SETTING.getKey(), leaderIndexMetaData.getIndex().getName());
        metadata.put(XDCRSettings.XDCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_SETTING.getKey(), remoteClusterAlias);
        imdBuilder.putCustom(XDCRSettings.XDCR_CUSTOM_METADATA_KEY, metadata);

        imdBuilder.settings(leaderIndexMetaData.getSettings());

        // Copy mappings from leader IMD to follow IMD
        for (ObjectObjectCursor<String, MappingMetaData> cursor : leaderIndexMetaData.getMappings()) {
            imdBuilder.putMapping(cursor.value);
        }

        imdBuilder.setRoutingNumShards(leaderIndexMetaData.getRoutingNumShards());
        // We assert that insync allocation ids are not empty in `PrimaryShardAllocator`
        for (IntObjectCursor<Set<String>> entry : leaderIndexMetaData.getInSyncAllocationIds()) {
            imdBuilder.putInSyncAllocationIds(entry.key, Collections.singleton(IN_SYNC_ALLOCATION_ID));
        }

        return imdBuilder.build();
    }

    @Override
    public RepositoryData getRepositoryData() {
        Client remoteClient = getRemoteClusterClient();
        ClusterStateResponse response = remoteClient.admin().cluster().prepareState().clear().setMetaData(true)
                .get(XDCRSettings.getRecoveryActionTimeout());
        MetaData remoteMetaData = response.getState().getMetaData();

        Map<String, SnapshotId> copiedSnapshotIds = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>(copiedSnapshotIds.size());
        Map<IndexId, Set<SnapshotId>> indexSnapshots = new HashMap<>(copiedSnapshotIds.size());

        ImmutableOpenMap<String, IndexMetaData> remoteIndices = remoteMetaData.getIndices();
        for (String indexName : remoteMetaData.getConcreteAllIndices()) {
            // Both the Snapshot name and UUID are set to _latest_
            SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
            copiedSnapshotIds.put(indexName, snapshotId);
            snapshotStates.put(indexName, SnapshotState.SUCCESS);
            Index index = remoteIndices.get(indexName).getIndex();
            indexSnapshots.put(new IndexId(indexName, index.getUUID()), Collections.singleton(snapshotId));
        }

        return new RepositoryData(1, copiedSnapshotIds, snapshotStates, indexSnapshots, Collections.emptyList());
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                         List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return throttledTime.count();
    }

    @Override
    public String startVerification() {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void endVerification(String verificationToken) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    @Override
    public void restoreShard(IndexShard indexShard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId,
                             RecoveryState recoveryState) {
        createEmptyStore(indexShard, shardId);

        final Map<String, String> xdcrMetaData = indexShard.indexSettings().getIndexMetaData().getCustomData(XDCRSettings.XDCR_CUSTOM_METADATA_KEY);
        final String leaderIndexName = xdcrMetaData.get(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_NAME_SETTING.getKey());
        final String leaderUUID = xdcrMetaData.get(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING.getKey());
        final Index leaderIndex = new Index(leaderIndexName, leaderUUID);
        final ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());

        final Client remoteClient = getRemoteClusterClient();

        final String retentionLeaseId =
                retentionLeaseId(localClusterName, indexShard.shardId().getIndex(), remoteClusterAlias, leaderIndex);

        // 如果只拷贝索引文件，无需检查远程SoftDeleted
        if (indexShard.indexSettings().isSoftDeleteEnabled()) {
            acquireRetentionLeaseOnLeader(shardId, retentionLeaseId, leaderShardId, remoteClient);
        }

        final Scheduler.Cancellable renewable = threadPool.scheduleWithFixedDelay(
                () -> {
                    logger.trace("{} background renewal of retention lease [{}] during restore", indexShard.shardId(), retentionLeaseId);
                    final ThreadContext threadContext = threadPool.getThreadContext();
                    try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                        threadContext.markAsSystemContext();
                        RetentionLeases.asyncRenewRetentionLease(
                                leaderShardId,
                                retentionLeaseId,
                                RETAIN_ALL,
                                remoteClient,
                                ActionListener.wrap(
                                        r -> {
                                        },
                                        e -> {
                                            assert e instanceof ElasticsearchSecurityException == false : e;
                                            logger.warn(new ParameterizedMessage(
                                                            "{} background renewal of retention lease [{}] failed during restore",
                                                            indexShard.shardId(),
                                                            retentionLeaseId),
                                                    e);
                                        }));
                    }
                },
                RetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(indexShard.indexSettings().getNodeSettings()),
                XDCRSettings.XDCR_THREAD_POOL_BULK);

        try (RestoreSession restoreSession = openSession(metadata.name(), remoteClient, leaderShardId, indexShard, recoveryState)) {
            restoreSession.restoreFiles();
            updateMappings(remoteClient, leaderIndex, restoreSession.mappingVersion, client, indexShard.routingEntry().index());
        } catch (Exception e) {
            throw new IndexShardRestoreFailedException(indexShard.shardId(), "failed to restore snapshot [" + snapshotId + "]", e);
        } finally {
            logger.trace("{} canceling background renewal of retention lease [{}] at the end of restore", shardId, retentionLeaseId);
            renewable.cancel();
        }
    }

    private void createEmptyStore(final IndexShard indexShard, final ShardId shardId) {
        final Store store = indexShard.store();
        store.incRef();
        try {
            store.createEmpty();
        } catch (final EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to create empty store", e);
        } finally {
            store.decRef();
        }
    }

    void acquireRetentionLeaseOnLeader(
            final ShardId shardId,
            final String retentionLeaseId,
            final ShardId leaderShardId,
            final Client remoteClient) {
        logger.trace(
                () -> new ParameterizedMessage("{} requesting leader to add retention lease [{}]", shardId, retentionLeaseId));
        final TimeValue timeout = XDCRSettings.getRecoveryActionTimeout();
        final Optional<RetentionLeaseAlreadyExistsException> maybeAddAlready =
                syncAddRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
        maybeAddAlready.ifPresent(addAlready -> {
            logger.trace(() -> new ParameterizedMessage(
                            "{} retention lease [{}] already exists, requesting a renewal",
                            shardId,
                            retentionLeaseId),
                    addAlready);
            final Optional<RetentionLeaseNotFoundException> maybeRenewNotFound =
                    syncRenewRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
            maybeRenewNotFound.ifPresent(renewNotFound -> {
                logger.trace(() -> new ParameterizedMessage(
                                "{} retention lease [{}] not found while attempting to renew, requesting a final add",
                                shardId,
                                retentionLeaseId),
                        renewNotFound);
                final Optional<RetentionLeaseAlreadyExistsException> maybeFallbackAddAlready =
                        syncAddRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, timeout);
                maybeFallbackAddAlready.ifPresent(fallbackAddAlready -> {
                    assert false : fallbackAddAlready;
                    throw fallbackAddAlready;
                });
            });
        });
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId leaderShardId) {
        throw new UnsupportedOperationException("Unsupported for repository of type: " + TYPE);
    }

    private void updateMappings(Client leaderClient, Index leaderIndex, long leaderMappingVersion,
                                Client followerClient, Index followerIndex) {
        final PlainActionFuture<IndexMetaData> indexMetadataFuture = new PlainActionFuture<>();
        final long startTimeInNanos = System.nanoTime();
        final Supplier<TimeValue> timeout = () -> {
            final long elapsedInNanos = System.nanoTime() - startTimeInNanos;
            return TimeValue.timeValueNanos(XDCRSettings.getRecoveryActionTimeout().nanos() - elapsedInNanos);
        };
        Requests.getIndexMetadata(leaderClient, leaderIndex, leaderMappingVersion, 0L, timeout, indexMetadataFuture);
        final IndexMetaData leaderIndexMetadata = indexMetadataFuture.actionGet(XDCRSettings.getRecoveryActionTimeout());
        if (leaderIndexMetadata.getMappings().isEmpty()) {
            assert leaderIndexMetadata.getMappingVersion() == 1;
        } else {
            assert leaderIndexMetadata.getMappings().size() == 1 : "expected exactly one mapping, but got [" +
                    leaderIndexMetadata.getMappings().size() + "]";
            MappingMetaData mappingMetaData = leaderIndexMetadata.getMappings().iterator().next().value;
            if (mappingMetaData != null) {
                final PutMappingRequest putMappingRequest = Requests.putMappingRequest(followerIndex.getName(), mappingMetaData)
                        .masterNodeTimeout(TimeValue.timeValueMinutes(30));
                followerClient.admin().indices().putMapping(putMappingRequest).actionGet(XDCRSettings.getRecoveryActionTimeout());
            }
        }
    }

    RestoreSession openSession(String repositoryName, Client remoteClient, ShardId leaderShardId, IndexShard indexShard,
                               RecoveryState recoveryState) {
        String sessionUUID = UUIDs.randomBase64UUID();
        PutRestoreSessionAction.PutRestoreSessionResponse response = remoteClient.execute(PutRestoreSessionAction.INSTANCE,
                new PutRestoreSessionRequest(sessionUUID, leaderShardId)).actionGet(XDCRSettings.getRecoveryActionTimeout());
        return new RestoreSession(repositoryName, remoteClient, sessionUUID, response.getNode(), indexShard, recoveryState,
                response.getStoreFileMetaData(), response.getMappingVersion(), threadPool, XDCRSettings, throttledTime::inc);
    }


    static class RestoreSession extends FileRestoreContext implements Closeable {

        final Client remoteClient;
        final String sessionUUID;
        final DiscoveryNode node;
        final Store.MetadataSnapshot sourceMetaData;
        final long mappingVersion;
        final org.elasticsearch.apack.XDCRSettings xdcrSettings;
        final LongConsumer throttleListener;
        final ThreadPool threadPool;

        RestoreSession(String repositoryName, Client remoteClient, String sessionUUID, DiscoveryNode node, IndexShard indexShard,
                       RecoveryState recoveryState, Store.MetadataSnapshot sourceMetaData, long mappingVersion,
                       ThreadPool threadPool, org.elasticsearch.apack.XDCRSettings xdcrSettings, LongConsumer throttleListener) {
            super(repositoryName, indexShard, SNAPSHOT_ID, recoveryState, Math.toIntExact(xdcrSettings.getChunkSize().getBytes()));
            this.remoteClient = remoteClient;
            this.sessionUUID = sessionUUID;
            this.node = node;
            this.sourceMetaData = sourceMetaData;
            this.mappingVersion = mappingVersion;
            this.threadPool = threadPool;
            this.xdcrSettings = xdcrSettings;
            this.throttleListener = throttleListener;
        }

        void restoreFiles() throws IOException {
            ArrayList<FileInfo> fileInfos = new ArrayList<>();
            for (StoreFileMetaData fileMetaData : sourceMetaData) {
                ByteSizeValue fileSize = new ByteSizeValue(fileMetaData.length());
                fileInfos.add(new FileInfo(fileMetaData.name(), fileMetaData, fileSize));
            }
            SnapshotFiles snapshotFiles = new SnapshotFiles(LATEST, fileInfos);
            restore(snapshotFiles);
        }

        @Override
        protected void restoreFiles(List<FileInfo> filesToRecover, Store store) {
            logger.trace("[{}] starting remote restore of {} files", shardId, filesToRecover);
            final List<StoreFileMetaData> mds = filesToRecover.stream().map(FileInfo::metadata).collect(Collectors.toList());
            CountDownLatch completedLatch = new CountDownLatch(1);
            SetOnce<Exception> error = new SetOnce<>();
            final MultiChunkTransfer<StoreFileMetaData, FileChunk> multiFileTransfer = new MultiChunkTransfer<StoreFileMetaData, FileChunk>(
                    logger,
                    threadPool.getThreadContext(),
                    ActionListener.wrap(
                            r -> completedLatch.countDown(),
                            e -> {
                                completedLatch.countDown();
                                error.set(e);
                            }),
                    xdcrSettings.getMaxConcurrentFileChunks(),
                    mds
            ) {

                final MultiFileWriter multiFileWriter = new MultiFileWriter(store, recoveryState.getIndex(), "", logger, () -> {
                });
                long offset = 0;

                @Override
                protected void onNewResource(StoreFileMetaData md) {
                    offset = 0;
                }

                @Override
                protected FileChunk nextChunkRequest(StoreFileMetaData md) {
                    final int bytesRequested = Math.toIntExact(Math.min(xdcrSettings.getChunkSize().getBytes(), md.length() - offset));
                    offset += bytesRequested;
                    return new FileChunk(md, bytesRequested, offset == md.length());
                }

                @Override
                protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
                    remoteClient.execute(
                            GetRestoreFileChunkAction.INSTANCE,
                            new GetRestoreFileChunkRequest(node, sessionUUID, request.md.name(), request.bytesRequested),
                            ListenerTimeouts.wrapWithTimeout(
                                    threadPool,
                                    new ActionListener<GetRestoreFileChunkAction.GetRestoreFileChunkResponse>() {
                                        @Override
                                        public void onResponse(GetRestoreFileChunkAction.GetRestoreFileChunkResponse chunkResponse
                                        ) {
                                            chunkResponse.incRef();
                                            threadPool.generic().execute(new ActionRunnable<Void>(listener) {
                                                @Override
                                                protected void doRun() throws Exception {
                                                    writeFileChunk(request.md, chunkResponse);
                                                    listener.onResponse(null);
                                                }

                                                @Override
                                                public void onAfter() {
                                                    chunkResponse.decRef();
                                                }
                                            });
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            threadPool.generic().execute(() -> {
                                                try {
                                                    listener.onFailure(e);
                                                } catch (Exception ex) {
                                                    e.addSuppressed(ex);
                                                    logger.warn(
                                                            () -> new ParameterizedMessage("failed to execute failure callback for chunk request"),
                                                            e
                                                    );
                                                }
                                            });
                                        }
                                    },
                                    xdcrSettings.getRecoveryActionTimeout(),
                                    ThreadPool.Names.GENERIC,
                                    GetRestoreFileChunkAction.NAME
                            )
                    );
                }

                private void writeFileChunk(StoreFileMetaData md, GetRestoreFileChunkAction.GetRestoreFileChunkResponse r)
                        throws Exception {
                    final int actualChunkSize = r.getChunk().length();
                    logger.trace(
                            "[{}] [{}] got response for file [{}], offset: {}, length: {}",
                            shardId,
                            snapshotId,
                            md.name(),
                            r.getOffset(),
                            actualChunkSize
                    );
                    final long nanosPaused = xdcrSettings.getRecoveryRateLimiter().maybePause(actualChunkSize);
                    throttleListener.accept(nanosPaused);
                    multiFileWriter.incRef();
                    try {
                        final boolean lastChunk = r.getOffset() + actualChunkSize >= md.length();
                        multiFileWriter.writeFileChunk(md, r.getOffset(), r.getChunk(), lastChunk);
                    } catch (Exception e) {
                        handleError(md, e);
                        throw e;
                    } finally {
                        multiFileWriter.decRef();
                    }
                }

                @Override
                protected void handleError(StoreFileMetaData md, Exception e) throws Exception {
                    final IOException corruptIndexException;
                    if ((corruptIndexException = ExceptionsHelper.unwrapCorruption(e)) != null) {
                        try {
                            store.markStoreCorrupted(corruptIndexException);
                        } catch (IOException ioe) {
                            logger.warn("store cannot be marked as corrupted", e);
                        }
                        throw corruptIndexException;
                    }
                    throw e;
                }

                @Override
                public void close() {
                    multiFileWriter.close();
                }
            };
            multiFileTransfer.start();

            try {
                completedLatch.await();
                if (error.get() != null) {
                    logger.error("{} restore failed ", shardId);
                    throw new ElasticsearchException(error.get());
                } else {
                    logger.info("{} restore completed ", shardId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread was interrupted while waiting for shard [" + shardId + "] to restore", e);
            }
        }

        @Override
        protected InputStream fileInputStream(FileInfo fileInfo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            ClearRestoreSessionRequest clearRequest = new ClearRestoreSessionRequest(sessionUUID, node);
            ClearRestoreSessionAction.ClearRestoreSessionResponse response = remoteClient.execute(ClearRestoreSessionAction.INSTANCE, clearRequest).actionGet(xdcrSettings.getRecoveryActionTimeout());
        }
    }

    private static class FileChunk implements MultiChunkTransfer.ChunkRequest {
        final StoreFileMetaData md;
        final int bytesRequested;
        final boolean lastChunk;

        FileChunk(StoreFileMetaData md, int bytesRequested, boolean lastChunk) {
            this.md = md;
            this.bytesRequested = bytesRequested;
            this.lastChunk = lastChunk;
        }

        @Override
        public boolean lastChunk() {
            return lastChunk;
        }
    }

    public void fetchLeaderHistoryUUIDs(
            final Client remoteClient,
            final IndexMetaData leaderIndexMetaData,
            final Consumer<Exception> onFailure,
            final Consumer<String[]> historyUUIDConsumer) {

        String leaderIndex = leaderIndexMetaData.getIndex().getName();
        CheckedConsumer<IndicesStatsResponse, Exception> indicesStatsHandler = indicesStatsResponse -> {
            IndexStats indexStats = indicesStatsResponse.getIndices().get(leaderIndex);
            if (indexStats == null) {
                onFailure.accept(new IllegalArgumentException("no index stats available for the leader index"));
                return;
            }

            String[] historyUUIDs = new String[leaderIndexMetaData.getNumberOfShards()];
            for (IndexShardStats indexShardStats : indexStats) {
                for (ShardStats shardStats : indexShardStats) {
                    if (shardStats.getShardRouting().primary() == false) {
                        continue;
                    }

                    CommitStats commitStats = shardStats.getCommitStats();
                    if (commitStats == null) {
                        onFailure.accept(new IllegalArgumentException("leader index's commit stats are missing"));
                        return;
                    }
                    String historyUUID = commitStats.getUserData().get(Engine.HISTORY_UUID_KEY);
                    ShardId shardId = shardStats.getShardRouting().shardId();
                    historyUUIDs[shardId.id()] = historyUUID;
                }
            }
            for (int i = 0; i < historyUUIDs.length; i++) {
                if (historyUUIDs[i] == null) {
                    onFailure.accept(new IllegalArgumentException("no history uuid for [" + leaderIndex + "][" + i + "]"));
                    return;
                }
            }
            historyUUIDConsumer.accept(historyUUIDs);
        };
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.indices(leaderIndex);
        remoteClient.admin().indices().stats(request, ActionListener.wrap(indicesStatsHandler, onFailure));
    }
}
