package org.elasticsearch.apack;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.apack.terms.TermsFetchAction;
import org.elasticsearch.apack.terms.TermsFetchRequest;
import org.elasticsearch.apack.terms.TermsFetchResponse;
import org.elasticsearch.apack.terms.collector.TermsReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinQueryBuilder extends AbstractQueryBuilder<JoinQueryBuilder> {

    public static final String NAME = "joined";
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField JOINED_FIELD = new ParseField("join");

    private QueryBuilder queryBuilder;
    // 传递termsQueryBuilders
    private List<TermsQueryBuilder> termsQueryBuilders;

    public JoinQueryBuilder(QueryBuilder queryBuilder, List<JoinClauseBuilder> joinClauseBuilders, Client client) {
        this.queryBuilder = queryBuilder;
        List<TermsQueryBuilder> filters = new ArrayList<>();
        if (joinClauseBuilders != null && !joinClauseBuilders.isEmpty()) {
            for (JoinClauseBuilder join : joinClauseBuilders) {
                TermsFetchRequest request = new TermsFetchRequest();
                request.indices(join.indices()).field(join.field()).source(buildSource(join.query()));
                TermsFetchResponse resp = client.execute(TermsFetchAction.INSTANCE, request).actionGet();
                if (resp.getShardFailures().length > 0) {
                    throw new RuntimeException(resp.getShardFailures()[0].getCause());
                }
                List terms = TermsReader.readList(resp.getEncodedTermsSet());
                TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(join.sourceField(), terms);
                filters.add(termsQueryBuilder);
            }
        }
        termsQueryBuilders = filters;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(queryBuilder.toQuery(context), BooleanClause.Occur.MUST);
        if (termsQueryBuilders != null && !termsQueryBuilders.isEmpty()) {
            for (TermsQueryBuilder termsQueryBuilder: termsQueryBuilders) {
                builder.add(termsQueryBuilder.toQuery(context), BooleanClause.Occur.MUST);
            }
        }
        return builder.build();
    }

    private static SearchSourceBuilder buildSource(QueryBuilder queryBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        return searchSourceBuilder;
    }

    @Override
    protected boolean doEquals(JoinQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    public JoinQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilder = in.readNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            termsQueryBuilders = in.readList(TermsQueryBuilder::new);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
        if (!termsQueryBuilders.isEmpty()) {
            out.writeBoolean(true);
            out.writeList(termsQueryBuilders);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (queryBuilder != null) {
            builder.field(QUERY_FIELD.getPreferredName());
            queryBuilder.toXContent(builder, params);
        }
        if (termsQueryBuilders != null && !termsQueryBuilders.isEmpty()) {
            builder.field(JOINED_FIELD.getPreferredName(), termsQueryBuilders);
        }
        builder.endObject();
    }

    public static JoinQueryBuilder fromXContent(XContentParser parser) throws IOException {

        String currentFieldName = null;
        XContentParser.Token token;
        QueryBuilder queryBuilder = null;
        List<JoinClauseBuilder> joinClauseBuilders = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryBuilder = parseInnerQueryBuilder(parser);
                } else if (JOINED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    joinClauseBuilders.addAll(JoinClauseBuilder.fromXContent(parser));
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[rank] query does not support [" + currentFieldName + "]");
                }
            }
        }
        return new JoinQueryBuilder(queryBuilder, joinClauseBuilders, JoinPlugin.client());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
