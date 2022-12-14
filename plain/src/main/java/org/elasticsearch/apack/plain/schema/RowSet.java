package org.elasticsearch.apack.plain.schema;

import java.util.function.Consumer;

/**
 * A set of rows to be returned at one time and a way
 * to get the next set of rows.
 */
public interface RowSet extends RowView {

    boolean hasCurrentRow();

    boolean advanceRow();

    // number or rows in this set; while not really necessary (the return of advanceRow works)
    int size();

    void reset();

    /**
     * The key used by PlanExecutor#nextPage to fetch the next page.
     */
    Cursor nextPageCursor();

    default void forEachRow(Consumer<? super RowView> action) {
        for (boolean hasRows = hasCurrentRow(); hasRows; hasRows = advanceRow()) {
            action.accept(this);
        }
    }
}
