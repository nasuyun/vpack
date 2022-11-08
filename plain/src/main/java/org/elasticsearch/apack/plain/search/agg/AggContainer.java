package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.schema.expression.FieldAttribute;
import org.elasticsearch.apack.plain.search.FieldExtraction;
import org.elasticsearch.apack.plain.search.agg.MetricAggRef;
import org.elasticsearch.apack.plain.type.KeywordEsField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.emptyList;

public class AggContainer {

    final List<Attribute> input;
    final List<FieldExtraction> output;
    final AggregatorFactories.Builder aggs;
    final int limit;

    public AggContainer(SearchRequest searchRequest, int limit) {
        this.aggs = searchRequest.source().aggregations();
        this.limit = limit;
        this.input = new ArrayList<>();
        this.output = new ArrayList<>();
        if (aggs != null && aggs.getAggregatorFactories().isEmpty() == false) {
            for (AggregationBuilder aggregator : aggs.getAggregatorFactories()) {

                if (aggregator instanceof MultiBucketAggregationBuilder) {
                }

                if (aggregator instanceof ValuesSourceAggregationBuilder.LeafOnly) {
                }

                String name = aggregator.getName();
                input.add(new FieldAttribute(name, new KeywordEsField(name)));
                output.add(new MetricAggRef(name, false));
            }
        }
    }

    public List<FieldExtraction> fields() {
        return output;
    }

    public int limit() {
        return limit;
    }

    public BitSet columnMask(List<Attribute> fields) {
        BitSet mask = new BitSet(fields.size());
        int index = 0;
        for (Attribute f : fields) {
            mask.set(index++);
        }
        return mask;
    }

    public List<Tuple<Integer, Comparator>> sortingColumns() {
        return emptyList();
    }
}
