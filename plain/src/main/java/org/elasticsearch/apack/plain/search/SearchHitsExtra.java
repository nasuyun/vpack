package org.elasticsearch.apack.plain.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.apack.plain.action.PlainSearchResponse;
import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.schema.expression.FieldAttribute;
import org.elasticsearch.apack.plain.schema.Rows;
import org.elasticsearch.apack.plain.search.extractor.FieldHitExtractor;
import org.elasticsearch.apack.plain.search.extractor.HitExtractor;
import org.elasticsearch.apack.plain.type.Schema;
import org.elasticsearch.search.SearchHit;

import java.time.ZoneId;
import java.util.*;

/**
 * ExtraHits 输出结果集
 */
public class SearchHitsExtra implements ResponseExtra {

    private final BitSet mask;
    private List<FieldExtraction> fields = new ArrayList<>();

    final Schema schema;
    final List<Attribute> attributes;
    final ZoneId zoneId;

    public SearchHitsExtra(List<Attribute> attributes, ZoneId zoneId) {
        this.attributes = attributes;
        this.zoneId = zoneId;
        this.schema = Rows.schema(attributes);
        this.mask = columnMask(attributes);
        for (Attribute attribute : attributes) {
            if (attribute instanceof FieldAttribute) {
                fields.add(topHitFieldRef((FieldAttribute) attribute));
            }
        }
    }

    @Override
    public PlainSearchResponse extraResponse(SearchResponse response, int limit) {
        SchemaSearchHitRowSet rowSet = extraSearchHitRowSet(response, limit);
        return createResponse(rowSet);
    }

    private FieldExtraction topHitFieldRef(FieldAttribute fieldAttr) {
        return new SearchHitFieldRef(fieldAttr.name(), fieldAttr.field().getDataType(), fieldAttr.field().isAggregatable());
    }

    private SchemaSearchHitRowSet extraSearchHitRowSet(SearchResponse response, int limit) {
        SearchHit[] hits = response.getHits().getHits();
        // create response extractors for the first time
        List<FieldExtraction> refs = fields;
        List<HitExtractor> exts = new ArrayList<>(refs.size());
        for (FieldExtraction ref : refs) {
            exts.add(createExtractor(ref));
        }
        String scrollId = response.getScrollId();
        SchemaSearchHitRowSet hitRowSet = new SchemaSearchHitRowSet(schema, exts, mask, hits, limit, scrollId);
        return hitRowSet;
    }

    public BitSet columnMask(List<Attribute> columns) {
        BitSet mask = new BitSet(fields.size());
        int index = 0;
        for (Attribute column : columns) {
            mask.set(index++);
        }
        return mask;
    }

    private HitExtractor createExtractor(FieldExtraction ref) {
        if (ref instanceof SearchHitFieldRef) {
            SearchHitFieldRef f = (SearchHitFieldRef) ref;
            return new FieldHitExtractor(
                    f.name(),
                    f.getDataType(),
                    zoneId,
                    f.useDocValue(),
                    f.hitName(), false);
        }
        throw new IllegalArgumentException("unkown " + ref);
    }

//    public static PlainSearchResponse createResponse(SchemaRowSet rowSet) {
//        List<ColumnInfo> columns = new ArrayList<>(rowSet.columnCount());
//        for (Schema.Entry entry : rowSet.schema()) {
//            columns.add(new ColumnInfo("", entry.name(), entry.type().typeName, entry.type().displaySize));
//        }
//        columns = unmodifiableList(columns);
//        return createResponse(rowSet, columns);
//    }
//
//    static PlainSearchResponse createResponse(RowSet rowSet, List<ColumnInfo> columns) {
//        List<List<Object>> rows = new ArrayList<>();
//        rowSet.forEachRow(rowView -> {
//            List<Object> row = new ArrayList<>(rowView.columnCount());
//            rowView.forEachColumn(row::add);
//            rows.add(unmodifiableList(row));
//        });
//        return new PlainSearchResponse(
//                Cursors.encodeToString(Version.CURRENT, rowSet.nextPageCursor()),
//                columns,
//                rows);
//    }
}
