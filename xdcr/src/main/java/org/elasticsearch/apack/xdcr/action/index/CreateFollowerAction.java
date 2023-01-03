package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.XDCRSettings;
import org.elasticsearch.apack.xdcr.repository.RemoteRepository;
import org.elasticsearch.apack.xdcr.utils.Actions;

import java.io.IOException;
import java.util.Objects;


/**
 * 创建备集群跟随索引
 * + snapshot_recovery 方式
 */
public class CreateFollowerAction extends Action<CreateFollowerAction.Request, CreateFollowerAction.Response, CreateFollowerAction.Builder> {

    public static final CreateFollowerAction INSTANCE = new CreateFollowerAction();
    public static final String NAME = "cluster:admin/xdcr/create_follower";

    private CreateFollowerAction() {
        super(NAME);
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of streamable is to be replaced by writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {

        private String repository;
        private IndexMetaData indexMetaData;

        private Request() {
        }

        public Request(String repository, IndexMetaData indexMetaData) {
            this.repository = repository;
            this.indexMetaData = indexMetaData;
        }

        public String index() {
            return indexMetaData.getIndex().getName();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            indexMetaData = IndexMetaData.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            indexMetaData.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return Actions.notNull("repository", repository, "index_meta_data", indexMetaData);
        }
    }

    /**
     * RequestBuilder
     */
    public static class Builder extends MasterNodeOperationRequestBuilder<Request, Response, Builder> {
        protected Builder(ElasticsearchClient client, Action<Request, Response, Builder> action, Request request) {
            super(client, action, request);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public final boolean followIndexCreated;
        public final boolean followIndexShardsAcked;
        public final boolean indexFollowingStarted;

        public Response(boolean followIndexCreated, boolean followIndexShardsAcked, boolean indexFollowingStarted) {
            this.followIndexCreated = followIndexCreated;
            this.followIndexShardsAcked = followIndexShardsAcked;
            this.indexFollowingStarted = indexFollowingStarted;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            followIndexCreated = in.readBoolean();
            followIndexShardsAcked = in.readBoolean();
            indexFollowingStarted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(followIndexCreated);
            out.writeBoolean(followIndexShardsAcked);
            out.writeBoolean(indexFollowingStarted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("follow_index_created", followIndexCreated);
                builder.field("follow_index_shards_acked", followIndexShardsAcked);
                builder.field("index_following_started", indexFollowingStarted);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return followIndexCreated == response.followIndexCreated &&
                    followIndexShardsAcked == response.followIndexShardsAcked &&
                    indexFollowingStarted == response.indexFollowingStarted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final RestoreService restoreService;

        @Inject
        public Transport(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ClusterService clusterService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final Client client,
                final RestoreService restoreService,
                final AllocationService allocationService) {
            super(
                    settings,
                    CreateFollowerAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    Request::new,
                    indexNameExpressionResolver);
            this.restoreService = restoreService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of streamable is to be replaced by writeable");
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(
                final Request request,
                final ClusterState state,
                final ActionListener<Response> listener) throws Exception {
            createFollowerIndex(request, listener);
        }

        /**
         * 创建跟随索引
         */
        private void createFollowerIndex(
                final Request request,
                final ActionListener<Response> listener) {

            IndexMetaData leaderIndexMetaData = request.indexMetaData;
            if (leaderIndexMetaData == null) {
                listener.onFailure(new IllegalArgumentException("leader index [" + request.index() + "] does not exist"));
                return;
            }

            final Settings.Builder settingsBuilder = Settings.builder()
                    .put(IndexMetaData.SETTING_INDEX_PROVIDED_NAME, request.index())
                    .put(XDCRSettings.XDCR_FOLLOWING_INDEX_SETTING.getKey(), true)
                    .put(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING.getKey(), leaderIndexMetaData.getIndexUUID())
                    .put(XDCRSettings.XDCR_CUSTOM_METADATA_LEADER_INDEX_NAME_SETTING.getKey(), leaderIndexMetaData.getIndex().getName())
                    .put(XDCRSettings.XDCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_SETTING.getKey(), request.repository);

            String repoName = RemoteRepository.NAME_PREFIX + request.repository;
            final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(repoName, RemoteRepository.LATEST)
                    .indices(request.index())
                    .masterNodeTimeout(TimeValue.timeValueMinutes(30))
                    .indexSettings(settingsBuilder);

            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                protected void doRun() {

                    /**
                     * 恢复一阶段索引数据
                     */
                    restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreService.RestoreCompletionResponse>() {
                        @Override
                        public void onResponse(RestoreService.RestoreCompletionResponse response) {
                            listener.onResponse(new Response(true, false, false));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
        }
    }

}
