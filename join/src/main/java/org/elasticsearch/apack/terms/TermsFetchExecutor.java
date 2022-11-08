package org.elasticsearch.apack.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.apack.terms.collector.TermsCollector;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class TermsFetchExecutor implements Callable<TermsFetchResult> {

    private static Logger logger = LogManager.getLogger(TermsFetchExecutor.class);

    private SearchContext searchContext;
    private IndexSearcher searcher;
    private TermsCollector termsCollector;
    private Consumer<Runnable> checkCancellationSetter;

    public TermsFetchExecutor(SearchContext searchContext, IndexSearcher searcher, TermsCollector termsCollector,
                              Consumer<Runnable> checkCancellationSetter) {
        this.searchContext = searchContext;
        this.searcher = searcher;
        this.termsCollector = termsCollector;
        this.checkCancellationSetter = checkCancellationSetter;
    }

    @Override
    public TermsFetchResult call() {
        searchContext.queryResult().searchTimedOut(false);
        TermsFetchResult queryResult = new TermsFetchResult();
        queryResult.searchTimedOut(false);
        try {
            Query query = searchContext.query();
            boolean timeoutSet = searchContext.timeout() != null && searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

            final Runnable timeoutRunnable;
            if (timeoutSet) {
                final Counter counter = searchContext.timeEstimateCounter();
                final long startTime = counter.get();
                final long timeout = searchContext.timeout().millis();
                final long maxTime = startTime + timeout;
                timeoutRunnable = () -> {
                    final long time = counter.get();
                    if (time > maxTime) {
                        throw new TimeExceededException();
                    }
                };
            } else {
                timeoutRunnable = null;
            }

            final Runnable cancellationRunnable;
            if (searchContext.lowLevelCancellation()) {
                SearchTask task = searchContext.getTask();
                cancellationRunnable = () -> {
                    if (task.isCancelled()) throw new TaskCancelledException("cancelled");
                };
            } else {
                cancellationRunnable = null;
            }

            final Runnable checkCancelled;
            if (timeoutRunnable != null && cancellationRunnable != null) {
                checkCancelled = () -> {
                    timeoutRunnable.run();
                    cancellationRunnable.run();
                };
            } else if (timeoutRunnable != null) {
                checkCancelled = timeoutRunnable;
            } else if (cancellationRunnable != null) {
                checkCancelled = cancellationRunnable;
            } else {
                checkCancelled = null;
            }

            checkCancellationSetter.accept(checkCancelled);
            long start = System.currentTimeMillis();
            try {
                searcher.search(query, termsCollector);
            } catch (TermsCollector.PrunedTerminationException e) {
                queryResult.pruned(true);
            } catch (TimeExceededException e) {
                assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                queryResult.searchTimedOut(true);
            } finally {
                searchContext.clearReleasables(SearchContext.Lifetime.COLLECTION);
            }
            logger.debug("terms took[{} ms] size[{}]", (System.currentTimeMillis() - start), termsCollector.terms().size());
            queryResult.terms(termsCollector.terms());
            return queryResult;
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }
    }

    private static class TimeExceededException extends RuntimeException {
    }
}
