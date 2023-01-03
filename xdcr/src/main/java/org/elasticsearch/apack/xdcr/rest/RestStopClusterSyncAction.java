package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.apack.xdcr.action.cluster.ClusterSyncRequest;
import org.elasticsearch.apack.xdcr.action.cluster.StopClusterSyncAction;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestStopClusterSyncAction extends BaseRestHandler {

    public RestStopClusterSyncAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_xdcr/{repository}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String repository = request.param("repository");
        final ClusterSyncRequest stopRequest = new ClusterSyncRequest(repository);
        return channel -> client.execute(StopClusterSyncAction.INSTANCE, stopRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_stop_cluster_sync_action";
    }

}
