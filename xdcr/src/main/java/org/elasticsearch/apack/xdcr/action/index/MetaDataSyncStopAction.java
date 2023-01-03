package org.elasticsearch.apack.xdcr.action.index;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.apack.xdcr.utils.ResponseHandler;

import java.io.IOException;

import static org.elasticsearch.apack.xdcr.action.index.MetaDataSyncStartAction.TASK_ID;

/**
 * 停止元数据同步任务
 */
public class MetaDataSyncStopAction extends Action<MetaDataSyncStopAction.Request, AcknowledgedResponse, MetaDataSyncStopAction.RequestBuilder> {

    public static final MetaDataSyncStopAction INSTANCE = new MetaDataSyncStopAction();
    public static final String NAME = "internal:cluster/xdcr/syncmeta/stop";

    private MetaDataSyncStopAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends MasterNodeRequest<Request> {


        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    /**
     * Transport
     */
    public static class Transport extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final PersistentTasksService persistentTasksService;

        @Inject
        public Transport(Settings settings, TransportService transportService, ThreadPool threadPool,
                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                         ClusterService clusterService, PersistentTasksService persistentTasksService, Client client) {
            super(settings, MetaDataSyncStopAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, Request::new);
            this.persistentTasksService = persistentTasksService;
        }


        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
            final ResponseHandler handler = new ResponseHandler(listener);
            persistentTasksService.sendRemoveRequest(TASK_ID, handler.getActionListener());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse newResponse() {
            return new AcknowledgedResponse();
        }


    }

}
