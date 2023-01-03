package org.elasticsearch.apack.xdcr.action.stats;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StatsAction extends Action<StatsAction.Request, StatsAction.Response, StatsAction.Builder> {

    public static final StatsAction INSTANCE = new StatsAction();
    public static final String NAME = "cluster:monitor/xdcr/stats";

    private StatsAction() {
        super(NAME);
    }

    @Override
    public Builder newRequestBuilder(ElasticsearchClient client) {
        return new Builder(client, INSTANCE, new Request());
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    /**
     * Request
     */
    public static class Request extends AcknowledgedRequest<Request> {

        private String repository = "";
        private String index = "";

        public Request() {
        }

        public String repository() {
            return repository;
        }

        public String index() {
            return index;
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
            return null;
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

        private List<Stats> stats;

        public Response(List<Stats> stats) {
            this.stats = stats;
        }

        public List<Stats> stats() {
            return stats;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.stats = in.readList(Stats::readStats);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(stats);
        }

        public String[] notPeerIndices() {
            if (stats == null || stats.isEmpty()) {
                return new String[]{};
            }
            return stats.stream()
                    .filter(v -> v.localSeqNoStats.getMaxSeqNo() != v.remoteSeqNoStats.getMaxSeqNo()
                            || (v.localSeqNoStats.getMaxSeqNo() == -1 || v.remoteSeqNoStats.getMaxSeqNo() == -1))
                    .map(s -> s.index)
                    .distinct()
                    .toArray(String[]::new);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String[] notPeerIndices = notPeerIndices();
            builder.startObject();
            builder.field("isPeer", (stats != null && stats.isEmpty() == false) && notPeerIndices.length == 0);
            builder.array("notPeer", notPeerIndices);
            {
                builder.startArray("stats");
                for (Stats s : stats) {
                    s.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, Response> {

        private final StatsCollector collector;

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
                    StatsAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    Request::new,
                    indexNameExpressionResolver);
            collector = new StatsCollector(client, indexNameExpressionResolver);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected Response newResponse() {
            throw new UnsupportedOperationException("usage of streamable is to be replaced by Writeable");
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(
                final Request request,
                final ClusterState state,
                final ActionListener<Response> listener) {

            listener.onResponse(collector.collect(request, state));
        }

        @Override
        protected ClusterBlockException checkBlock(final Request request, final ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.toString());
        }
    }

}
