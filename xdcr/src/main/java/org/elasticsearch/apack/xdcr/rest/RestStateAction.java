package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.apack.xdcr.action.stats.StateAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestStateAction extends BaseRestHandler {

    public RestStateAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_xdcr/state", this);
        controller.registerHandler(GET, "/_xdcr/state/{repository}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return channel -> client.execute(StateAction.INSTANCE, new StateAction.Request(), new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_stats_action";
    }

}
