package org.elasticsearch.apack.terms;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescorerBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SearchContextFactory {

    final static SearchContext createContext(SearchService searchService, ShardSearchRequest request, TimeValue timeout) throws IOException {
        final SearchContext context = searchService.createSearchContext(request, timeout);
        try {
            if (request.scroll() != null) {
                context.scrollContext(new ScrollContext());
                context.scrollContext().scroll = request.scroll();
            }
            parseSource(context, request.source());

            // if the from and size are still not set, default them
            if (context.from() == -1) {
                context.from(0);
            }
            if (context.size() == -1) {
                context.size(10);
            }
        } catch (Exception e) {
            context.close();
            throw ExceptionsHelper.convertToRuntime(e);
        }

        return context;
    }

    // TODO more spec disable
    private static void parseSource(SearchContext context, SearchSourceBuilder source) throws SearchContextException {
        // nothing to parse...
        if (source == null) {
            return;
        }
        QueryShardContext queryShardContext = context.getQueryShardContext();
        context.from(source.from());
        context.size(source.size());
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        if (source.query() != null) {
            InnerHitContextBuilder.extractInnerHits(source.query(), innerHitBuilders);
            context.parsedQuery(queryShardContext.toQuery(source.query()));
        }
        if (source.postFilter() != null) {
            InnerHitContextBuilder.extractInnerHits(source.postFilter(), innerHitBuilders);
            context.parsedPostFilter(queryShardContext.toQuery(source.postFilter()));
        }
        if (innerHitBuilders.size() > 0) {
            for (Map.Entry<String, InnerHitContextBuilder> entry : innerHitBuilders.entrySet()) {
                try {
                    entry.getValue().build(context, context.innerHits());
                } catch (IOException e) {
                    throw new SearchContextException(context, "failed to build inner_hits", e);
                }
            }
        }
        if (source.sorts() != null) {
            throw new SearchContextException(context, "`sort` cannot be used in a term query context.");
        }
        context.trackScores(source.trackScores());
        if (source.trackTotalHits() == false && context.scrollContext() != null) {
            throw new SearchContextException(context, "disabling [track_total_hits] is not allowed in a scroll context");
        }
        context.trackTotalHits(source.trackTotalHits());
        if (source.minScore() != null) {
            context.minimumScore(source.minScore());
        }
        if (source.timeout() != null) {
            context.timeout(source.timeout());
        }
        context.terminateAfter(source.terminateAfter());
        if (source.aggregations() != null) {
            throw new SearchContextException(context, "`agg` cannot be used in a term query context.");
        }
        if (source.suggest() != null) {
            try {
                context.suggest(source.suggest().build(queryShardContext));
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create SuggestionSearchContext", e);
            }
        }
        if (source.rescores() != null) {
            try {
                for (RescorerBuilder<?> rescore : source.rescores()) {
                    context.addRescore(rescore.buildContext(queryShardContext));
                }
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create RescoreSearchContext", e);
            }
        }
        if (source.explain() != null) {
            context.explain(source.explain());
        }
        if (source.fetchSource() != null) {
            context.fetchSourceContext(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            int maxAllowedDocvalueFields = context.mapperService().getIndexSettings().getMaxDocvalueFields();
            if (source.docValueFields().size() > maxAllowedDocvalueFields) {
                throw new IllegalArgumentException(
                        "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + maxAllowedDocvalueFields
                                + "] but was [" + source.docValueFields().size() + "]. This limit can be set by changing the ["
                                + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting.");
            }
            context.docValueFieldsContext(new DocValueFieldsContext(source.docValueFields()));
        }
        if (source.highlighter() != null) {
            HighlightBuilder highlightBuilder = source.highlighter();
            try {
                context.highlight(highlightBuilder.build(queryShardContext));
            } catch (IOException e) {
                throw new SearchContextException(context, "failed to create SearchContextHighlighter", e);
            }
        }
        if (source.scriptFields() != null) {
            throw new SearchContextException(context, "`scripte` cannot be used in a term query context.");
        }
        if (source.ext() != null) {
            for (SearchExtBuilder searchExtBuilder : source.ext()) {
                context.addSearchExt(searchExtBuilder);
            }
        }
        if (source.version() != null) {
            context.version(source.version());
        }
        if (source.stats() != null) {
            context.groupStats(source.stats());
        }
        if (source.searchAfter() != null && source.searchAfter().length > 0) {
            throw new SearchContextException(context, "`search_after` cannot be used in a term query context.");
        }

        if (source.slice() != null) {
            throw new SearchContextException(context, "`slice` cannot be used in a term query context.");
        }

        if (source.storedFields() != null) {
            if (source.storedFields().fetchFields() == false) {
                if (context.version()) {
                    throw new SearchContextException(context, "`stored_fields` cannot be disabled if version is requested");
                }
                if (context.sourceRequested()) {
                    throw new SearchContextException(context, "`stored_fields` cannot be disabled if _source is requested");
                }
            }
            context.storedFieldsContext(source.storedFields());
        }

        if (source.collapse() != null) {
            final CollapseContext collapseContext = source.collapse().build(context);
            context.collapse(collapseContext);
        }
    }
}
