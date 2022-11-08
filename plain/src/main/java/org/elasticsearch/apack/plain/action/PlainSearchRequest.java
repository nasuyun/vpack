package org.elasticsearch.apack.plain.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.apack.plain.TextFormat;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.ZoneId;

public class PlainSearchRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private ZoneId zoneId;
    private String[] columns;
    private TextFormat format;

    private SearchRequest searchRequest;

    public ZoneId zoneId() {
        return zoneId;
    }

    public String[] columns() {
        return columns;
    }

    public TextFormat format() {
        return format;
    }

    public SearchRequest searchRequest() {
        return searchRequest;
    }

    public void zoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public void columns(String[] columns) {
        this.columns = columns;
    }

    public void format(TextFormat format) {
        this.format = format;
    }

    public void searchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.zoneId = ZoneId.of(in.readString());
        this.columns = in.readStringArray();
        this.format = TextFormat.fromMediaTypeOrFormat(in.readString());
        this.searchRequest = new SearchRequest();
        this.searchRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(zoneId.getId());
        out.writeStringArray(columns);
        out.writeString(format.toString());
        searchRequest.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return searchRequest.validate();
    }

    @Override
    public IndicesRequest indices(String... indices) {
        return searchRequest.indices(indices);
    }

    @Override
    public String[] indices() {
        return searchRequest.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return searchRequest.indicesOptions();
    }
}
