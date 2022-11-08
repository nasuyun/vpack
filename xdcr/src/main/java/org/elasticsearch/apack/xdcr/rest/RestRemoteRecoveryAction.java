/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.apack.xdcr.action.index.RemoteRecoveryAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * 恢复远程索引，不做增量更新。
 */
public class RestRemoteRecoveryAction extends BaseRestHandler {

    public RestRemoteRecoveryAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(PUT, "/_xdcr/_recovery/{repository}/{index}", this);
        controller.registerHandler(POST, "/_xdcr/_recovery/{repository}/{index}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String repository = request.param("repository");
        final String index = request.param("index");
        //  request.paramAsBoolean("wait_for_completion", false)
        final RemoteRecoveryAction.Request recoveryRequest = new RemoteRecoveryAction.Request(repository, index);
        return channel -> client.execute(RemoteRecoveryAction.INSTANCE, recoveryRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "xdcr_remote_recovery_action";
    }

}
