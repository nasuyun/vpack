package org.elasticsearch.apack.xdcr.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 系统用户能处理的 Action
 * internal:*
 * <p>
 * 其他Action通过授权用户处理
 */
public class Ssl {

    private static final Logger logger = LogManager.getLogger(Ssl.class);

    public static boolean isXpackSslEnabled(Settings settings) {
        return settings.getAsBoolean("xpack.security.transport.ssl.enabled", false);
    }

    /**
     * 获取远程客户端
     *
     * @param client     client
     * @param repository repository
     * @return remote client
     */
    public static Client remoteClient(Client client, String repository) {
        return client.getRemoteClusterClient(repository);
    }

    /**
     * Wrapper systemuser context in client
     *
     * @param client client
     * @return SystemContextClient
     */
    public static Client systemClient(Client client) {
        return (client instanceof SystemClient) ?
                client : new SystemClient(client);
    }

    /**
     * Wrapper UserContext on Client
     *
     * @param client  client
     * @param headers headers
     * @return Client
     */
    public static Client userClient(Client client, Map<String, String> headers) {
        return (client instanceof AuthedClient) ?
                client : new AuthedClient(client, headers);
    }

    private static class SystemClient extends FilterClient {
        final ThreadContext threadContext;

        public SystemClient(Client in) {
            super(in);
            threadContext = in.threadPool().getThreadContext();
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
        void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }

    private static class AuthedClient extends FilterClient {
        final ThreadContext threadContext;
        final Map<String, String> headers;

        public AuthedClient(Client in, Map<String, String> header) {
            super(in);
            this.threadContext = in.threadPool().getThreadContext();
            this.headers = filteredHeaders(header);
        }

        protected <Request extends ActionRequest, Response extends ActionResponse,
                RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>>
        void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, headers)) {
                super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }

    public static final Map<String, String> filteredHeaders(Map<String, String> headers) {
        return headers.entrySet().stream()
                .filter(e -> HEADER_FILTERS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static final Set<String> HEADER_FILTERS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("es-security-runas-user", "_xpack_security_authentication")));

}
