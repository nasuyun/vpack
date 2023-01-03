package org.elasticsearch.apack.plain.search;

import org.elasticsearch.apack.plain.schema.Cursor;
import org.elasticsearch.apack.plain.schema.ListRowSet;
import org.elasticsearch.apack.plain.type.Schema;

import java.util.List;

public class PagingListRowSet extends ListRowSet {

    private final int pageSize;
    private final int columnCount;
    private final Cursor cursor;

    PagingListRowSet(List<List<?>> list, int columnCount, int pageSize) {
        this(Schema.EMPTY, list, columnCount, pageSize);
    }

    public PagingListRowSet(Schema schema, List<List<?>> list, int columnCount, int pageSize) {
        super(schema, list);
        this.columnCount = columnCount;
        this.pageSize = Math.min(pageSize, list.size());
        this.cursor = list.size() > pageSize ? new PagingListCursor(list, columnCount, pageSize) : Cursor.EMPTY;
    }

    @Override
    public int size() {
        return pageSize;
    }

    @Override
    public int columnCount() {
        return columnCount;
    }

    @Override
    public Cursor nextPageCursor() {
        return cursor;
    }
}
