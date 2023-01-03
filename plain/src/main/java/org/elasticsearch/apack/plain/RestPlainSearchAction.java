
package org.elasticsearch.apack.plain;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.apack.plain.action.PlainSearchAction;
import org.elasticsearch.apack.plain.action.PlainSearchRequest;
import org.elasticsearch.apack.plain.action.PlainSearchResponse;
import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;

public class RestPlainSearchAction extends BaseRestHandler {

    public static final String TYPED_KEYS_PARAM = "typed_keys";
    public static final String TOTAL_HIT_AS_INT_PARAM = "rest_total_hits_as_int";
    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(Arrays.asList(TYPED_KEYS_PARAM, TOTAL_HIT_AS_INT_PARAM));
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    public RestPlainSearchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_query", this);
        controller.registerHandler(POST, "/_query", this);
        controller.registerHandler(GET, "/{index}/_query", this);
        controller.registerHandler(POST, "/{index}/_query", this);
        controller.registerHandler(GET, "/{index}/{type}/_query", this);
        controller.registerHandler(POST, "/{index}/{type}/_query", this);
    }

    @Override
    public String getName() {
        return "plain_search_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        AtomicReference<RestRequest> replacedRequestRef = new AtomicReference<>();
        AtomicReference<PlainSearchRequest> rormatSearchRequestRef = new AtomicReference<>();
        RestRequestReplacer.onReplace(request,
                searchActionRequest -> rormatSearchRequestRef.set(searchActionRequest),
                newRequest -> replacedRequestRef.set(newRequest)
        );
        PlainSearchRequest plainSearchRequest = rormatSearchRequestRef.get();
        RestRequest restRequest = replacedRequestRef.get();

        IntConsumer setSize = size -> searchRequest.source().size(size);
        restRequest.withContentOrSourceParamParserOrNull(parser ->
                parseSearchRequest(searchRequest, restRequest, parser, setSize));

        TextFormat textFormat = plainSearchRequest.format();
        plainSearchRequest.searchRequest(searchRequest);
        long startNanos = System.nanoTime();
        return channel -> client.execute(PlainSearchAction.INSTANCE, plainSearchRequest, new RestResponseListener<PlainSearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(PlainSearchResponse response) {
                Cursor cursor = Cursor.EMPTY;
                final String data = textFormat.format(cursor, response);
//                textFormat.contentType(restRequest)
                RestResponse restResponse = new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, data.getBytes(StandardCharsets.UTF_8));
                restResponse.addHeader("Took-nanos", Long.toString(System.nanoTime() - startNanos));
                return restResponse;
            }
        });
    }

    /**
     * Parses the rest request on top of the SearchRequest, preserving values that are not overridden by the rest request.
     *
     * @param requestContentParser body of the request to read. This method does not attempt to read the body from the {@code request}
     *                             parameter
     * @param setSize              how the size url parameter is handled. {@code udpate_by_search2} and regular search differ here.
     */
    public static void parseSearchRequest(SearchRequest searchRequest, RestRequest request,
                                          XContentParser requestContentParser,
                                          IntConsumer setSize) throws IOException {

        if (searchRequest.source() == null) {
            searchRequest.source(new SearchSourceBuilder());
        }
        searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        if (requestContentParser != null) {
            searchRequest.source().parseXContent(requestContentParser, true);
        }

        final int batchedReduceSize = request.paramAsInt("batched_reduce_size", searchRequest.getBatchedReduceSize());
        searchRequest.setBatchedReduceSize(batchedReduceSize);
        searchRequest.setPreFilterShardSize(request.paramAsInt("pre_filter_shard_size", searchRequest.getPreFilterShardSize()));

        if (request.hasParam("max_concurrent_shard_requests")) {
            // only set if we have the parameter since we auto adjust the max concurrency on the coordinator
            // based on the number of nodes in the cluster
            final int maxConcurrentShardRequests = request.paramAsInt("max_concurrent_shard_requests",
                    searchRequest.getMaxConcurrentShardRequests());
            searchRequest.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }

        if (request.hasParam("allow_partial_search_results")) {
            // only set if we have the parameter passed to override the cluster-level default
            searchRequest.allowPartialSearchResults(request.paramAsBoolean("allow_partial_search_results", null));
        }

        // do not allow 'query_and_fetch' or 'dfs_search2_and_fetch' search types
        // from the REST layer. these modes are an internal optimization and should
        // not be specified explicitly by the user.
        String searchType = request.param("search_type");
        if ("query_and_fetch".equals(searchType) ||
                "dfs_search2_and_fetch".equals(searchType)) {
            throw new IllegalArgumentException("Unsupported search type [" + searchType + "]");
        } else {
            searchRequest.searchType(searchType);
        }
        parseSearchSource(searchRequest.source(), request, setSize);
        searchRequest.requestCache(request.paramAsBoolean("request_cache", null));

        String scroll = request.param("scroll");
        if (scroll != null) {
            searchRequest.scroll(new Scroll(parseTimeValue(scroll, null, "scroll")));
        }

        searchRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        searchRequest.routing(request.param("routing"));
        searchRequest.preference(request.param("preference"));
        searchRequest.indicesOptions(IndicesOptions.fromRequest(request, searchRequest.indicesOptions()));
    }

    /**
     * Parses the rest request on top of the SearchSourceBuilder, preserving
     * values that are not overridden by the rest request.
     */
    private static void parseSearchSource(final SearchSourceBuilder searchSourceBuilder, RestRequest request, IntConsumer setSize) {
        QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
        if (queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }

        int from = request.paramAsInt("from", -1);
        if (from != -1) {
            searchSourceBuilder.from(from);
        }
        int size = request.paramAsInt("size", -1);
        if (size != -1) {
            setSize.accept(size);
        }

        if (request.hasParam("explain")) {
            searchSourceBuilder.explain(request.paramAsBoolean("explain", null));
        }
        if (request.hasParam("version")) {
            searchSourceBuilder.version(request.paramAsBoolean("version", null));
        }
        if (request.hasParam("seq_no_primary_term")) {
            searchSourceBuilder.seqNoAndPrimaryTerm(request.paramAsBoolean("seq_no_primary_term", null));
        }
        if (request.hasParam("timeout")) {
            searchSourceBuilder.timeout(request.paramAsTime("timeout", null));
        }
        if (request.hasParam("terminate_after")) {
            int terminateAfter = request.paramAsInt("terminate_after",
                    SearchContext.DEFAULT_TERMINATE_AFTER);
            if (terminateAfter < 0) {
                throw new IllegalArgumentException("terminateAfter must be > 0");
            } else if (terminateAfter > 0) {
                searchSourceBuilder.terminateAfter(terminateAfter);
            }
        }

        StoredFieldsContext storedFieldsContext =
                StoredFieldsContext.fromRestRequest(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), request);
        if (storedFieldsContext != null) {
            searchSourceBuilder.storedFields(storedFieldsContext);
        }
        String sDocValueFields = request.param("docvalue_fields");
        if (sDocValueFields != null) {
            if (Strings.hasText(sDocValueFields)) {
                String[] sFields = Strings.splitStringByCommaToArray(sDocValueFields);
                for (String field : sFields) {
                    searchSourceBuilder.docValueField(field, null);
                }
            }
        }
        FetchSourceContext fetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        if (fetchSourceContext != null) {
            searchSourceBuilder.fetchSource(fetchSourceContext);
        }

        if (request.hasParam("track_scores")) {
            searchSourceBuilder.trackScores(request.paramAsBoolean("track_scores", false));
        }

        if (request.hasParam("track_total_hits")) {
            searchSourceBuilder.trackTotalHits(request.paramAsBoolean("track_total_hits", true));
        }

        String sSorts = request.param("sort");
        if (sSorts != null) {
            String[] sorts = Strings.splitStringByCommaToArray(sSorts);
            for (String sort : sorts) {
                int delimiter = sort.lastIndexOf(":");
                if (delimiter != -1) {
                    String sortField = sort.substring(0, delimiter);
                    String reverse = sort.substring(delimiter + 1);
                    if ("asc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.ASC);
                    } else if ("desc".equals(reverse)) {
                        searchSourceBuilder.sort(sortField, SortOrder.DESC);
                    }
                } else {
                    searchSourceBuilder.sort(sort);
                }
            }
        }

        String sStats = request.param("stats");
        if (sStats != null) {
            searchSourceBuilder.stats(Arrays.asList(Strings.splitStringByCommaToArray(sStats)));
        }

        String suggestField = request.param("suggest_field");
        if (suggestField != null) {
            String suggestText = request.param("suggest_text", request.param("q"));
            int suggestSize = request.paramAsInt("suggest_size", 5);
            String suggestMode = request.param("suggest_mode");
            searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion(suggestField,
                    termSuggestion(suggestField)
                            .text(suggestText).size(suggestSize)
                            .suggestMode(SuggestMode.resolve(suggestMode))));
        }
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

}
