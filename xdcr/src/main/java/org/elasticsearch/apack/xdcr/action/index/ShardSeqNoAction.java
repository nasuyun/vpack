package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.apack.XDCRSettings.XDCR_THREAD_POOL_BULK;

/**
 * 获取Shard SeqNo
 */
public class ShardSeqNoAction extends Action<ShardSeqNoAction.Request, ShardSeqNoAction.Response, ShardSeqNoAction.RequestBuilder> {

    public static final ShardSeqNoAction INSTANCE = new ShardSeqNoAction();

    public static final String NAME = "indices:data/read/xdcr/get_seqno";

    public ShardSeqNoAction() {
        super(NAME);
    }

    @Override
    public ShardSeqNoAction.RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public ShardSeqNoAction.Response newResponse() {
        return new Response();
    }

    public static class Request extends SingleShardRequest<Request> implements IndicesRequest {

        int shardId;

        public Request() {
        }

        public Request(String index, int shardId) {
            super(index);
            this.shardId = shardId;
        }

        public int shardId() {
            return shardId;
        }


        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.shardId = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(shardId);
        }
    }

    public static class Response extends ActionResponse {

        long segNo;

        public Response(long segNo) {
            this.segNo = segNo;
        }

        public Response() {

        }

        public long getSegNo() {
            return segNo;
        }

        public Response(StreamInput in) throws IOException {
            readFrom(in);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            segNo = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(segNo);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {


        protected RequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action) {
            super(client, action, new Request());
        }
    }

    public static class TransportShardSegNoAction extends TransportSingleShardAction<Request, Response> {

        IndicesService indexServices;

        @Inject
        public TransportShardSegNoAction(Settings settings, ThreadPool threadPool,
                                         ClusterService clusterService, TransportService transportService,
                                         ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         IndicesService indexServices) {
            super(settings, ShardSeqNoAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, Request::new, XDCR_THREAD_POOL_BULK);
            this.indexServices = indexServices;
        }

        @Override
        protected Response shardOperation(Request request, ShardId shardId) throws IOException {
            IndexShard indexShard = indexServices.getShardOrNull(shardId);
            return new Response(indexShard.getGlobalCheckpoint());
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(Request request) {
            return true;
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            IndexShardRoutingTable indexShardRoutingTable =
                    state.getRoutingTable().index(request.request().index()).shard(request.request().shardId());
            return indexShardRoutingTable.primaryShard().shardsIt();
        }
    }


}
