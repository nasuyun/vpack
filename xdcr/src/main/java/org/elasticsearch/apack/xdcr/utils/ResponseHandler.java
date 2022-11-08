/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack.xdcr.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.concurrent.atomic.AtomicReference;

public final class ResponseHandler<T> {

    private final AtomicReference<Object> responses;
    private final ActionListener<AcknowledgedResponse> listener;

    public ResponseHandler(ActionListener<AcknowledgedResponse> listener) {
        this.responses = new AtomicReference();
        this.listener = listener;
    }

    public <T> ActionListener<T> getActionListener() {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T response) {
                responses.set(response);
                finalizeResponse();
            }

            @Override
            public void onFailure(Exception e) {
                responses.set(e);
                finalizeResponse();
            }
        };
    }

    private void finalizeResponse() {
        Exception error = null;
        Object response = responses.get();
        if (response instanceof Exception) {
            if (error == null) {
                error = (Exception) response;
            } else {
                error.addSuppressed((Exception) response);
            }
        }

        if (error == null) {
            listener.onResponse(new AcknowledgedResponse(true));
        } else {
            listener.onFailure(error);
        }
    }
}
