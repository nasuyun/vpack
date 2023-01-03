
package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.PlainIllegalArgumentException;
import org.elasticsearch.apack.plain.action.PlainSearchResponse;
import org.elasticsearch.apack.plain.schema.Configuration;
import org.elasticsearch.apack.plain.schema.Rows;
import org.elasticsearch.apack.plain.schema.SchemaRowSet;
import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.search.FieldExtraction;
import org.elasticsearch.apack.plain.search.ResponseExtra;
import org.elasticsearch.apack.plain.search.extractor.AggsExtractor;
import org.elasticsearch.apack.plain.search.extractor.CommonAggExtractor;
import org.elasticsearch.apack.plain.search.extractor.agg.*;
import org.elasticsearch.apack.plain.type.Schema;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static java.util.Collections.singletonList;

public class AggExtra implements ResponseExtra {

    Schema schema;
    BitSet mask;
    final ZoneId zoneId;
    final AggContainer query;

    public AggExtra(ZoneId zoneId, SearchRequest searchRequest, int limit) {
        this.zoneId = zoneId;
        this.query = new AggContainer(searchRequest, limit);
        this.schema = Rows.schema(query.input);
        this.mask = query.columnMask(query.input);
    }

    public PlainSearchResponse extraResponse(SearchResponse response, int limit) {
        List<AggsExtractor> extractors = buildAggsExtractors();
//        List<AggsExtractor> extractors = buildBucketExtractors();
        SchemaAggsRowSet rowSet = new SchemaAggsRowSet(schema, extractors, mask, response.getAggregations());
        return createResponse(rowSet);
    }

    private List<AggsExtractor> buildAggsExtractors() {
        List<FieldExtraction> refs = query.output;
        List<AggsExtractor> exts = new ArrayList<>(refs.size());
        for (FieldExtraction ref : refs) {
            exts.add(createAggExtractor(ref));
        }
        return exts;
    }

    private AggsExtractor createAggExtractor(FieldExtraction ref) {
        if (ref instanceof MetricAggRef) {
            MetricAggRef r = (MetricAggRef) ref;
            return new CommonAggExtractor(r.name(), zoneId);
        }
        throw new PlainIllegalArgumentException("Unexpected value reference {}", ref.getClass());
    }


    protected List<AggsExtractor> buildBucketExtractors() {
        // create response extractors for the first time
        List<FieldExtraction> refs = query.fields();
        List<AggsExtractor> exts = new ArrayList<>(refs.size());
        for (FieldExtraction ref : refs) {
            exts.add(createExtractor(ref));
        }
        return exts;
    }

    private BucketExtractor createExtractor(FieldExtraction ref) {
        if (ref instanceof GroupByRef) {
            GroupByRef r = (GroupByRef) ref;
            return new CompositeKeyExtractor(r.key(), r.property(), zoneId, r.isDateTimeBased());
        }

        if (ref instanceof MetricAggRef) {
            MetricAggRef r = (MetricAggRef) ref;
            return new MetricAggExtractor(r.name(), r.property(), r.innerKey(), zoneId, r.isDateTimeBased());
        }

        if (ref instanceof TopHitsAggRef) {
            TopHitsAggRef r = (TopHitsAggRef) ref;
            return new TopHitsAggExtractor(r.name(), r.fieldDataType(), zoneId);
        }

        throw new PlainIllegalArgumentException("Unexpected value reference {}", ref.getClass());
    }

    /**
     * Dedicated listener for composite aggs/group-by results.
     */
    public static class AggsActionListener extends BaseAggActionListener {

        ActionListener<SchemaRowSet> listener;

        protected AggsActionListener(Configuration cfg, List<Attribute> output, AggContainer query, ActionListener<SchemaRowSet> listener) {
            super(cfg, output, query);
            this.listener = listener;
        }

        @Override
        public void onResponse(SearchResponse response) {
//            if (response.getAggregations().asList().isEmpty() == false) {
//                listener.onResponse(
//                        new SchemaCompositeAggsRowSet(
//                                schema,
//                                initBucketExtractors(response), mask, response,
//                                query.sortingColumns().isEmpty() ? query.limit() : -1));
//            }
//            // no results
//            else {
//                listener.onResponse(Rows.empty(schema));
//            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    abstract static class BaseAggActionListener implements ActionListener<SearchResponse> {
        final AggContainer query;
        final Schema schema;
        final BitSet mask;
        final Configuration cfg;

        BaseAggActionListener(Configuration cfg, List<Attribute> output, AggContainer query) {
            this.cfg = cfg;
            this.query = query;
            this.schema = Rows.schema(output);
            this.mask = query.columnMask(output);
        }

        protected List<BucketExtractor> initBucketExtractors(SearchResponse response) {
            // create response extractors for the first time
            List<FieldExtraction> refs = query.fields();

            List<BucketExtractor> exts = new ArrayList<>(refs.size());
            ConstantExtractor totalCount = new ConstantExtractor(response.getHits().getTotalHits());
            for (FieldExtraction ref : refs) {
                exts.add(createExtractor(ref, totalCount));
            }
            return exts;
        }

        private BucketExtractor createExtractor(FieldExtraction ref, BucketExtractor totalCount) {
            if (ref instanceof GroupByRef) {
                GroupByRef r = (GroupByRef) ref;
                return new CompositeKeyExtractor(r.key(), r.property(), cfg.zoneId(), r.isDateTimeBased());
            }

            if (ref instanceof MetricAggRef) {
                MetricAggRef r = (MetricAggRef) ref;
                return new MetricAggExtractor(r.name(), r.property(), r.innerKey(), cfg.zoneId(), r.isDateTimeBased());
            }

            if (ref instanceof TopHitsAggRef) {
                TopHitsAggRef r = (TopHitsAggRef) ref;
                return new TopHitsAggExtractor(r.name(), r.fieldDataType(), cfg.zoneId());
            }

//            if (ref instanceof ComputedRef) {
//                Pipe proc = ((ComputedRef) ref).processor();
//
//                // wrap only agg inputs
//                proc = proc.transformDown(l -> {
//                    BucketExtractor be = createExtractor(l.context(), totalCount);
//                    return new AggExtractorInput(l.source(), l.expression(), l.action(), be);
//                }, AggPathInput.class);
//
//                return new ComputingExtractor(proc.asProcessor());
//            }

            throw new PlainIllegalArgumentException("Unexpected value reference {}", ref.getClass());
        }
    }

    private static List<? extends MultiBucketsAggregation.Bucket> EMPTY_BUCKET = singletonList(new MultiBucketsAggregation.Bucket() {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            throw new PlainIllegalArgumentException("No group-by/aggs defined");
        }

        @Override
        public Object getKey() {
            throw new PlainIllegalArgumentException("No group-by/aggs defined");
        }

        @Override
        public String getKeyAsString() {
            throw new PlainIllegalArgumentException("No group-by/aggs defined");
        }

        @Override
        public long getDocCount() {
            throw new PlainIllegalArgumentException("No group-by/aggs defined");
        }

        @Override
        public Aggregations getAggregations() {
            throw new PlainIllegalArgumentException("No group-by/aggs defined");
        }
    });
}
