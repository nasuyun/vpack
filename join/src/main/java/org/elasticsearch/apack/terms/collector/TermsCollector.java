package org.elasticsearch.apack.terms.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

abstract public class TermsCollector<T> implements Collector {

    public static final class PrunedTerminationException extends RuntimeException {
        PrunedTerminationException(String msg) {
            super(msg);
        }
    }

    protected String field;
    private TermsSet<T> termsSet;
    private int limit;

    protected TermsCollector(String field, TermsSet container, int limit) {
        this.field = field;
        this.termsSet = container;
        this.limit = limit;
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    protected void collectValue(T value) {
        termsSet.add(value);
        if (termsSet.size() >= limit) {
            throw new PrunedTerminationException("join field[" + field + "]" + "out of limit[" + limit + "]");
        }
    }

    public TermsSet<T> terms() {
        return termsSet;
    }

    /**
     * Terms LeafCollector
     */
    interface TermsLeafCollector extends LeafCollector {

        default void setScorer(Scorer scorer) throws IOException {
        }

        void collect(int doc) throws IOException;
    }

    /**
     * ======================================================
     * ----------- --- TermsCollector implements -- ---------
     * ======================================================
     */

    /**
     * Bytes Field TermsCollector
     */
    public static class BytesTermsCollector extends TermsCollector<BytesRef> {
        private ValuesSource.Bytes valuesSource;

        public BytesTermsCollector(String field, ValuesSource.Bytes valuesSource, CircuitBreaker circuitBreaker, int termsFetchLimit) {
            super(field, new BytesRefTermsSet(circuitBreaker), termsFetchLimit);
            this.valuesSource = valuesSource;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedBinaryDocValues values = valuesSource.bytesValues(context);
            return (TermsLeafCollector) doc -> {
                values.advanceExact(doc);
                for (int i = 0; i < values.docValueCount(); ++i) {
                    collectValue(values.nextValue());
                }
            };
        }
    }

    /**
     * Long Field TermsCollector
     */
    static class LongTermsCollector extends TermsCollector<Long> {
        private ValuesSource.Numeric valuesSource;

        public LongTermsCollector(String field, ValuesSource.Numeric valuesSource, CircuitBreaker circuitBreaker, int termsFetchLimit) {
            super(field, new LongTermsSet(circuitBreaker), termsFetchLimit);
            this.valuesSource = valuesSource;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedNumericDocValues values = valuesSource.longValues(context);
            return (TermsLeafCollector) doc -> {
                values.advanceExact(doc);
                for (int i = 0; i < values.docValueCount(); ++i) {
                    collectValue(values.nextValue());
                }
            };
        }
    }

    /**
     * Long Field TermsCollector
     */
    static class IntegerTermsCollector extends TermsCollector<Integer> {
        private ValuesSource.Numeric valuesSource;

        public IntegerTermsCollector(String field, ValuesSource.Numeric valuesSource, CircuitBreaker circuitBreaker, int termsFetchLimit) {
            super(field, new IntegerTermsSet(circuitBreaker), termsFetchLimit);
            this.valuesSource = valuesSource;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            SortedNumericDocValues values = valuesSource.longValues(context);
            return (TermsLeafCollector) doc -> {
                values.advanceExact(doc);
                for (int i = 0; i < values.docValueCount(); ++i) {
                    collectValue((int) values.nextValue());
                }
            };
        }
    }


    public static TermsCollector create(CircuitBreaker circuitBreaker, SearchContext context, MappedFieldType fieldType, int termsFetchLimit) throws IOException {
        String field = fieldType.name();
        ValuesSourceConfig config = ValuesSourceConfig.resolve(context.getQueryShardContext(), null, field, null, null, null, null);
        ValuesSource valuesSource = config.toValuesSource(context.getQueryShardContext());
        if (valuesSource instanceof ValuesSource.Bytes) {
            return new BytesTermsCollector(field, (ValuesSource.Bytes) valuesSource, circuitBreaker, termsFetchLimit);
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            if (fieldType.typeName().equals("integer")) {
                return new IntegerTermsCollector(field, (ValuesSource.Numeric) valuesSource, circuitBreaker, termsFetchLimit);
            } else if (fieldType.typeName().equals("long")) {
                return new LongTermsCollector(field, (ValuesSource.Numeric) valuesSource, circuitBreaker, termsFetchLimit);
            } else {
                throw new UnsupportedOperationException("terms cannot be applied to field [" + config.fieldContext().field()
                        + "]. It can not be applied to double float or boolean.");
            }
        }
        throw new UnsupportedOperationException("terms collector cannot be applied to field [" + config.fieldContext().field()
                + "]. It can only be applied to numeric or string fields.");
    }

}

