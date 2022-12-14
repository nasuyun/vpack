
package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.apack.plain.search.agg.Aggs.ROOT_GROUP_NAME;

/**
 * Reference to a sub/nested/metric aggregation.
 * Due to how ES query works, this is _always_ a child aggregation with the grouping (composite agg) as the parent.
 */
public class MetricAggRef extends AggRef {

    private final String name;
    private final String property;
    private final String innerKey;
    private final boolean isDateTimeBased;

    public MetricAggRef(String name, boolean isDateTimeBased) {
        this(name, "value", isDateTimeBased);
    }

    public MetricAggRef(String name, String property, boolean isDateTimeBased) {
        this(name, property, null, isDateTimeBased);
    }

    public MetricAggRef(String name, String property, String innerKey, boolean isDateTimeBased) {
        this.name = name;
        this.property = property;
        this.innerKey = innerKey;
        this.isDateTimeBased = isDateTimeBased;
    }

    public String name() {
        return name;
    }

    public String property() {
        return property;
    }

    public String innerKey() {
        return innerKey;
    }

    public boolean isDateTimeBased() {
        return isDateTimeBased;
    }

    @Override
    public String toString() {
        String i = innerKey != null ? "[" + innerKey + "]" : "";
        return ROOT_GROUP_NAME + ">" + name + "." + property + i;
    }

    @Override
    public void collectFields(SearchSourceBuilder sourceBuilder) {

    }
}
