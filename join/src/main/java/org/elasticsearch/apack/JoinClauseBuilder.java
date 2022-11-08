

package org.elasticsearch.apack;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JoinClauseBuilder implements Writeable, ToXContentObject, NamedWriteable {

    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField KEYON_FIELD = new ParseField("field");
    public static final ParseField QUERY_FIELD = new ParseField("query");

    private String sourceField;
    private String[] indices;
    private String field;
    private QueryBuilder query;

    public JoinClauseBuilder(String sourceField, String[] indices, String field, QueryBuilder queryBuilder) {
        this.sourceField = sourceField;
        this.indices = indices;
        this.field = field;
        this.query = queryBuilder;
    }

    public JoinClauseBuilder(StreamInput in) throws IOException {
        this.sourceField = in.readString();
        this.indices = in.readStringArray();
        this.field = in.readString();
        this.query = in.readNamedWriteable(QueryBuilder.class);
    }

    public String sourceField() {
        return sourceField;
    }

    public String[] indices() {
        return indices;
    }

    public String field() {
        return field;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceField);
        out.writeStringArray(indices);
        out.writeString(field);
        query.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("join");
        builder.startObject(sourceField);
        builder.field("indices", indices);
        builder.field("field", field);
        builder.field("query", query);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static List<JoinClauseBuilder> fromXContent(XContentParser parser) throws IOException {
        List<JoinClauseBuilder> joinFields = new ArrayList<>(2);
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    parseCompoundSortField(parser, joinFields);
                } else {
                    throw new IllegalArgumentException("malformed sort format, "
                            + "within the sort array, an object, or an actual string are allowed");
                }
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            parseCompoundSortField(parser, joinFields);
        } else {
            throw new IllegalArgumentException("malformed sort format, either start with array, object, or an actual string");
        }
        return joinFields;
    }

    private static void parseCompoundSortField(XContentParser parser, List<JoinClauseBuilder> joinFields)
            throws IOException {
        joinFields.add(buildSingle(parser));
    }

    private static JoinClauseBuilder buildSingle(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String sourceField = null;
        QueryBuilder query = null;
        String[] indices = null;
        String targetField = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields("join", parser.getTokenLocation(), sourceField, currentFieldName);
                sourceField = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            query = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
                        }
                    } else if (token.isValue()) {
                        if (INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            indices = Strings.splitStringByCommaToArray(parser.text());
                        } else if (KEYON_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            targetField = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            }
        }
        JoinClauseBuilder queryBuilder = new JoinClauseBuilder(sourceField, indices, targetField, query);
        return queryBuilder;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JoinClauseBuilder)) {
            return false;
        }

        JoinClauseBuilder o = (JoinClauseBuilder) other;
        return ((sourceField == null && o.sourceField == null) || sourceField.equals(o.sourceField))
                && indices == o.indices && o.field == field && query == o.query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sourceField, this.indices, this.field, this.query);
    }


    private static void throwParsingExceptionOnMultipleFields(String queryName, XContentLocation contentLocation,
                                                              String processedFieldName, String currentFieldName) {
        if (processedFieldName != null) {
            throw new ParsingException(contentLocation, "[" + queryName + "] query doesn't support multiple fields, found ["
                    + processedFieldName + "] and [" + currentFieldName + "]");
        }
    }

    @Override
    public String getWriteableName() {
        return "join";
    }
}
