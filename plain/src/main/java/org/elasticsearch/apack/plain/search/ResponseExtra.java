package org.elasticsearch.apack.plain.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.action.PlainSearchResponse;
import org.elasticsearch.apack.plain.schema.Cursors;
import org.elasticsearch.apack.plain.schema.RowSet;
import org.elasticsearch.apack.plain.schema.SchemaRowSet;
import org.elasticsearch.apack.plain.search.agg.ColumnInfo;
import org.elasticsearch.apack.plain.type.Schema;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public interface ResponseExtra {

    PlainSearchResponse extraResponse(SearchResponse response, int limit);

    default PlainSearchResponse createResponse(SchemaRowSet rowSet) {
        List<ColumnInfo> columns = new ArrayList<>(rowSet.columnCount());
        for (Schema.Entry entry : rowSet.schema()) {
            columns.add(new ColumnInfo("", entry.name(), entry.type().typeName, entry.type().displaySize));
        }
        columns = unmodifiableList(columns);
        return createResponse(rowSet, columns);
    }

    static PlainSearchResponse createResponse(RowSet rowSet, List<ColumnInfo> columns) {
        List<List<Object>> rows = new ArrayList<>();
        rowSet.forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.columnCount());
            rowView.forEachColumn(row::add);
            rows.add(unmodifiableList(row));
        });
        return new PlainSearchResponse(
                Cursors.encodeToString(Version.CURRENT, rowSet.nextPageCursor()),
                columns,
                rows);
    }
}
