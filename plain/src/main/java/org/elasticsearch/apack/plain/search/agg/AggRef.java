
package org.elasticsearch.apack.plain.search.agg;

import org.elasticsearch.apack.plain.search.FieldExtraction;

/**
 * Reference to a ES aggregation (which can be either a GROUP BY or Metric agg).
 */
public abstract class AggRef implements FieldExtraction {

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return true;
    }
}
