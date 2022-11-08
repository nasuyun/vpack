package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.apack.xdcr.utils.Actions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.apack.xdcr.action.index.RemoteRecoveryAction.*;
import static org.elasticsearch.apack.xdcr.utils.Ssl.remoteClient;
import static org.elasticsearch.apack.xdcr.utils.Ssl.userClient;
import static org.elasticsearch.threadpool.ThreadPool.Names.SNAPSHOT;

public class RemoteRecoveryAction extends Action<Request, Response, Builder> {

    public static final RemoteRecoveryAction INSTANCE = new RemoteRecoveryAction();
    public static final String NAME = "cluster:admin/xdcr/remote_recovery";

    private RemoteRecoveryAction() {
        super(NAME);
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of streamable is to be replaced by wWriteable");
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String repository;
        private String index;

        private Request() {
        }

        public Request(String repository, String index) {
            this.repository = repository;
            this.index = index;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            repository = in.readString();
            index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(repository);
            out.writeString(index);
        }

        @Override
        public ActionRequestValidationException validate() {
            return Actions.notNull("repository", repository, "index_meta_data", index);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final boolean followIndexCreated;
        private final boolean followIndexShardsAcked;
        private final boolean indexFollowingStarted;

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

    public static class Builder extends MasterNodeOperationRequestBuilder<Request, Response, Builder> {
        protected Builder(ElasticsearchClient client, Action<Request, Response, Builder> action, Request request) {
            super(client, action, request);
        }
    }

    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final Client client;

        @Inject
        public Transport(
                final Settings settings,
                final ThreadPool threadPool,
                final TransportService transportService,
                final ClusterService clusterService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final Client client) {
            super(
                    settings,
                    RemoteRecoveryAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    Request::new,
                    indexNameExpressionResolver);

            this.client = client;
        }

        @Override
        protected String executor() {
            return SNAPSHOT;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of streamable is to be replaced by writeable");
        }

        @Override
        protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

            String repository = request.repository;
            String index = request.index;
            // 获取远程索引MetaData
            Client remoteClient = userClient(remoteClient(client, repository), threadPool.getThreadContext().getHeaders());
            ClusterStateResponse remoteResponse = remoteClient.admin().cluster().prepareState().clear().setMetaData(true).setIndices(index).get();
            IndexMetaData indexMetaData = remoteResponse.getState().metaData().index(index);
            if (indexMetaData == null) {
                listener.onFailure(new IndexNotFoundException("remote index not found", index));
            }

            // 远程先flush索引，完成索引文件的持久化。
            remoteClient.admin().indices().prepareFlush(request.index).get();

            // TODO Remote index set readOnly

            // 执行恢复
            CreateFollowerAction.Request createFollowerrequest = new CreateFollowerAction.Request(repository, indexMetaData);
            client.execute(CreateFollowerAction.INSTANCE, createFollowerrequest,
                    new ActionListener<CreateFollowerAction.Response>() {
                        @Override
                        public void onResponse(CreateFollowerAction.Response response) {
                            listener.onResponse(new Response(response.followIndexCreated, response.followIndexShardsAcked, response.indexFollowingStarted));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState clusterState) {
            return clusterState.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index);
        }
    }


}
