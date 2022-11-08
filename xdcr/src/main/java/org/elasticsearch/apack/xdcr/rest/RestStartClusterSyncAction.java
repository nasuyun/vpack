/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.apack.xdcr.action.cluster.ClusterSyncRequest;
import org.elasticsearch.apack.xdcr.action.cluster.StartClusterSyncAction;

import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestStartClusterSyncAction extends BaseRestHandler {

    public RestStartClusterSyncAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(PUT, "/_xdcr/{repository}", this);
        controller.registerHandler(POST, "/_xdcr/{repository}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String repository = request.param("repository");
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.content(), false, request.getXContentType()).v2();
        final String includes = (String) sourceAsMap.get("includes");
        final String excludes = (String) sourceAsMap.get("excludes");
        final ClusterSyncRequest startJobRequest = new ClusterSyncRequest(repository,
                includes == null ? "" : includes,
                excludes == null ? "" : excludes);
        return channel -> client.execute(StartClusterSyncAction.INSTANCE, startJobRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_start_cluster_sync_action";
    }

}
