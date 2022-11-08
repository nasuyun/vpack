package org.elasticsearch.apack.xdcr.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;

import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class Actions {

    private static final Logger logger = LogManager.getLogger(Actions.class);

    public static <K, V> ActionRequestValidationException notNull(K k1, V v1) {
        ActionRequestValidationException validationException = null;
        if (v1 == null) {
            addValidationError(k1 + "params missing:", validationException);
        }
        return validationException;
    }

    public static <K, V> ActionRequestValidationException notNull(K k1, V v1, K k2, V v2) {
        ActionRequestValidationException validationException = null;
        if (v1 == null) {
            addValidationError(k1 + " params missing ", validationException);
        }
        if (v2 == null) {
            addValidationError(k2 + " params missing ", validationException);
        }
        return validationException;
    }

    public static <Response> ActionListener<Response> success(Consumer<Response> consumer) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                consumer.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
            }
        };
    }

    public static <Response> ActionListener<Response> failure(Consumer<Exception> consumer) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                // no-op
            }

            @Override
            public void onFailure(Exception e) {
                consumer.accept(e);
            }
        };
    }

    public static <Response> ActionListener<Response> always(Consumer consumer) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                consumer.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
                consumer.accept(e);
            }
        };
    }

}
