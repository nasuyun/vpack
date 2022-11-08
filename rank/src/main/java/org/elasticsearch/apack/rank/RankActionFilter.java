package org.elasticsearch.apack.rank;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.apack.rank.exception.PositionOutOfRangesException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RankActionFilter implements ActionFilter {

    @Override
    public int order() {
        return 100;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        if (!SearchAction.INSTANCE.name().equals(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        final SearchRequest searchRequest = (SearchRequest) request;
        final ActionListener<Response> wrappedListener = wrapActionListener(searchRequest, listener);
        chain.proceed(task, action, request, wrappedListener == null ? listener : wrappedListener);
    }


    public <Response extends ActionResponse> ActionListener<Response> wrapActionListener(final SearchRequest request,
                                                                                         final ActionListener<Response> listener) {
        switch (request.searchType()) {
            case DFS_QUERY_THEN_FETCH:
            case QUERY_THEN_FETCH:
                break;
            default:
                return null;
        }

        if (request.scroll() != null) {
            return null;
        }

        final SearchSourceBuilder source = request.source();
        if (source == null) {
            return null;
        }

        final String[] indices = request.indices();
        if (indices == null || indices.length != 1) {
            return null;
        }

        if (!(source.query() instanceof RankQueryBuilder)) {
            return null;
        }

        RankQueryBuilder rankQueryBuilder = (RankQueryBuilder) source.query();

        List<Position> positions = rankQueryBuilder.positions();
        if (positions == null || positions.size() == 0) {
            return null;
        }

        final int size = getInt(source.size(), 10);
        final int from = getInt(source.from(), 0);
        if (size < 0 || from < 0) {
            return null;
        }

        for (Position position : positions) {
            if (position.pos < 0) {
                throw new PositionOutOfRangesException("postion must > 0");
            }
        }
        final ActionListener<Response> searchResponseListener = createSearchResponseListener(listener, positions);
        return new ActionListener<Response>() {
            @Override
            public void onResponse(final Response response) {
                searchResponseListener.onResponse(response);
            }

            @Override
            public void onFailure(final Exception e) {
                searchResponseListener.onFailure(e);
            }
        };
    }

    private <Response extends ActionResponse> ActionListener<Response> createSearchResponseListener(
            final ActionListener<Response> listener, final List<Position> positions) {
        return new ActionListener<Response>() {

            @SuppressWarnings("unchecked")
            @Override
            public void onResponse(final Response response) {
                final SearchResponse searchResponse = (SearchResponse) response;
                final long totalHits = searchResponse.getHits().getTotalHits();
                if (totalHits == 0) {
                    listener.onResponse(response);
                    return;
                }

                SearchHit[] searchHits = searchResponse.getHits().getHits();
                Map<Integer, List<Integer>> invertedRule = inverted(positions);
                SearchHit[] hits = shuffle(searchHits, invertedRule);
                SearchHits newHits = new SearchHits(hits, searchResponse.getHits().totalHits, searchResponse.getHits().getMaxScore());

                SearchResponseSections internalResponse = new SearchResponseSections(
                        newHits,
                        searchResponse.getAggregations(),
                        searchResponse.getSuggest(),
                        searchResponse.isTimedOut(),
                        searchResponse.isTerminatedEarly(),
                        new SearchProfileShardResults(searchResponse.getProfileResults()),
                        searchResponse.getNumReducePhases());
                SearchResponse newResp = new SearchResponse(internalResponse,
                        searchResponse.getScrollId(),
                        searchResponse.getTotalShards(),
                        searchResponse.getSuccessfulShards(),
                        searchResponse.getSkippedShards(),
                        searchResponse.getTook().millis(),
                        searchResponse.getShardFailures(),
                        searchResponse.getClusters());

                listener.onResponse((Response) newResp);
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        };
    }

    /**
     * 倒置Position列表
     * <p>
     * [
     * {"id1", 2},
     * {"id2", 3},
     * {"id3", 1},
     * {"id4", 2}
     * ]
     * <p>
     * 倒转至Map
     * <p>
     * {
     * {1, [2]},
     * {4, [0,3]},
     * {6, [1]}
     * }
     */
    private static Map<Integer, List<Integer>> inverted(List<Position> positions) {
        Map<Integer, List<Integer>> map = new TreeMap<>((k1, k2) -> k1 - k2);
        for (int i = 0; i < positions.size(); i++) {
            Position p = positions.get(i);
            int po = p.pos - 1;
            if (map.containsKey(po)) {
                map.get(po).add(i);
            } else {
                List<Integer> list = new ArrayList<>();
                list.add(i);
                map.put(po, list);
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] shuffle(T[] arry, Map<Integer, List<Integer>> rules) {
        T[] tmp = (T[]) java.lang.reflect.Array.newInstance(arry.getClass().getComponentType(), arry.length);
        boolean[] used = new boolean[arry.length];
        int startOffset = 0;
        for (Integer key : rules.keySet()) {
            List<Integer> value = rules.get(key);
            for (int i = 0; i < value.size(); i++) {
                startOffset++;
                tmp[key + i] = arry[value.get(i)];
                used[key + i] = true;
            }
        }
        int j = 0;
        for (int i = startOffset; i < arry.length; i++) {
            while (used[j] == true) j++;
            tmp[j++] = arry[i];
        }
        return tmp;
    }


    private int getInt(final Object value, final int defaultValue) {
        if (value instanceof Number) {
            final int v = ((Number) value).intValue();
            if (v < 0) {
                return defaultValue;
            }
            return v;
        } else if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }
        return defaultValue;
    }

}
