/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.apack.xdcr.action.stats.StatsAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestStatsAction extends BaseRestHandler {

    public RestStatsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_xdcr/stats", this);
        controller.registerHandler(POST, "/_xdcr/stats", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.content(), false, request.getXContentType()).v2();
            final String repository = (String) sourceAsMap.get("repository");
            final String index = (String) sourceAsMap.get("index");
            StatsAction.Request actionRequest = new StatsAction.Request(repository, index);
            return channel -> client.execute(StatsAction.INSTANCE, actionRequest, new RestToXContentListener<>(channel));
        } else {
            return channel -> client.execute(StatsAction.INSTANCE, new StatsAction.Request(), new RestToXContentListener<>(channel));
        }
    }

    @Override
    public String getName() {
        return "xdcr_stats_action";
    }

}
