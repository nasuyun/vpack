/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.apack.xdcr.action.index.DeleteFollowJobAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestStopIndexSyncAction extends BaseRestHandler {

    public RestStopIndexSyncAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/_xdcr/{repository}/{index}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String repository = request.param("repository");
        final String index = request.param("index");
        final DeleteFollowJobAction.Request removeRequest = new DeleteFollowJobAction.Request(repository, index);
        return channel -> client.execute(DeleteFollowJobAction.INSTANCE, removeRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_stop_index_sync_action";
    }

}
