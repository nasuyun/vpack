package org.elasticsearch.apack.rank;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.apack.rank.exception.ParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class RankQueryBuilder extends AbstractQueryBuilder<RankQueryBuilder> {

    public static final String NAME = "ranked";
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField RANK_FIELD = new ParseField("rank");
    public static final ParseField RANK_TOP_FIELD = new ParseField("top");
    public static final ParseField RANK_POS_FIELD = new ParseField("pos");
    public static final ParseField RANK_BLOCK_FIELD = new ParseField("block");

    public static final float TOP_SCORE_FACTOR = 1000;
    public static final float POS_SCORE_FACTOR = 10000;

    private final QueryBuilder query;
    private final String[] topIds;
    private final String[] blockIds;
    private final List<Position> positions;

    public RankQueryBuilder(QueryBuilder query, String[] topIds, String[] blockIds, List<Position> positions) {
        this.query = query == null ? new MatchAllQueryBuilder() : query;
        this.topIds = topIds;
        this.blockIds = blockIds;
        this.positions = positions;
        validation();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // rewrite query
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(query.toQuery(context), BooleanClause.Occur.MUST);
        if (blockIds != null && blockIds.length > 0) {
            Query query = buildIdQuery(context, blockIds, 0);
            builder.add(query, BooleanClause.Occur.MUST_NOT);
        }
        if (topIds != null && topIds.length > 0) {
            for (int i = 0; i < topIds.length; i++) {
                Query topQuery = buildIdQuery(context, new String[]{topIds[i]}, TOP_SCORE_FACTOR * (topIds.length - i));
                builder.add(topQuery, BooleanClause.Occur.SHOULD);
            }
        }
        if (positions != null) {
            for (int i = 0; i < positions.size(); i++) {
                Query positionQuery = buildIdQuery(context, new String[]{positions.get(i).id}, POS_SCORE_FACTOR * (positions.size() - i));
                builder.add(positionQuery, BooleanClause.Occur.SHOULD);
            }
        }
        return builder.build();
    }

    public List<Position> positions() {
        return positions;
    }

    public RankQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            topIds = in.readStringArray();
        } else {
            topIds = null;
        }
        if (in.readBoolean()) {
            blockIds = in.readStringArray();
        } else {
            blockIds = null;
        }
        if (in.readBoolean()) {
            positions = in.readList(Position::new);
        } else {
            positions = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(query);
        if (topIds != null) {
            out.writeBoolean(true);
            out.writeStringArray(topIds);
        } else {
            out.writeBoolean(false);
        }
        if (blockIds != null) {
            out.writeBoolean(true);
            out.writeStringArray(blockIds);
        } else {
            out.writeBoolean(false);
        }
        if (positions != null && !positions.isEmpty()) {
            out.writeBoolean(true);
            out.writeList(positions);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName());
            query.toXContent(builder, params);
        }
        builder.field(RANK_FIELD.getPreferredName());
        if (topIds != null && topIds.length > 0) {
            builder.field(RANK_TOP_FIELD.getPreferredName(), topIds);
        }
        if (blockIds != null && blockIds.length > 0) {
            builder.field(RANK_BLOCK_FIELD.getPreferredName(), blockIds);
        }
        if (positions != null && positions.size() > 0) {
            builder.field(RANK_POS_FIELD.getPreferredName(), positions);
        }
        builder.endObject();
    }

    @Override
    protected boolean doEquals(RankQueryBuilder other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return Objects.equals(this.query, other.query) &&
                Objects.equals(this.topIds, other.topIds) &&
                Objects.equals(this.blockIds, other.blockIds) &&
                Objects.equals(this.positions, other.positions);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, topIds, blockIds, positions);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private void validation() {
        if (positions != null && (positions.isEmpty() == false)) {
            Collection<String> posIds = positions.stream().map(k -> k.id).collect(Collectors.toList());
            if (blockIds != null) {
                for (int i = 0; i < blockIds.length; i++) {
                    if (posIds.contains(blockIds[i])) {
                        throw new ParseException("conflict id between block and pos [{}]", blockIds[i]);
                    }
                }
            }
        }
    }

    // static methods

    public static RankQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        QueryBuilder qb = null;
        String queryName = null;

        String[] topIds = null;
        String[] blockIds = null;
        List<Position> positions = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    qb = parseInnerQueryBuilder(parser);
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (RANK_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    // parse rank rules
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else {
                            if (RANK_TOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (token == XContentParser.Token.START_ARRAY) {
                                    List<String> ids = new ArrayList<>();
                                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        String path = parser.text();
                                        ids.add(path);
                                    }
                                    topIds = ids.toArray(new String[ids.size()]);
                                }
                            } else if (RANK_BLOCK_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (token == XContentParser.Token.START_ARRAY) {
                                    List<String> ids = new ArrayList<>();
                                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        String path = parser.text();
                                        ids.add(path);
                                    }
                                    blockIds = ids.toArray(new String[ids.size()]);
                                }
                            } else if (RANK_POS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (token == XContentParser.Token.START_ARRAY) {
                                    positions = new ArrayList<>();
                                    while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                        Map<String, Object> posMap = parser.map();
                                        for (Entry<String, Object> entry : posMap.entrySet()) {
                                            positions.add(new Position(entry.getKey(), (Integer) entry.getValue()));
                                        }
                                    }
                                }
                            } else {
                                throw new ParsingException(parser.getTokenLocation(), "[ranked] query does not support [" + currentFieldName + "]");
                            }
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[rank] query does not support [" + currentFieldName + "]");
                }
            }
        }
        RankQueryBuilder queryBuilder = new RankQueryBuilder(qb, topIds, blockIds, positions);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    private static Query buildIdQuery(QueryShardContext context, String[] ids, float boost) throws IOException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        // index1#id1, index2#id1, index3#id1
        List<String> uids = Arrays.stream(ids).filter(c -> c.contains("#")).collect(Collectors.toList());
        // id1, id2, id3
        List<String> sids = Arrays.stream(ids).filter(c -> !c.contains("#")).collect(Collectors.toList());
        if (!uids.isEmpty()) {
            Query uidQuery = uidQuery(context, uids, boost);
            builder.add(uidQuery, BooleanClause.Occur.SHOULD);
        }
        if (!sids.isEmpty()) {
            String[] idArray = sids.toArray(new String[sids.size()]);
            Query sidQuery = new TermsQueryBuilder("_id", idArray).boost(boost).toQuery(context);
            builder.add(sidQuery, BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    // 包含#的ID
    private static Query uidQuery(QueryShardContext context, List<String> uids, float boost) throws IOException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String id : uids) {
            String[] uid = id.split("#");
            TermQueryBuilder indexQuery = new TermQueryBuilder("_index", uid[0]);
            TermQueryBuilder idQuery = new TermQueryBuilder("_id", uid[1]);
            BooleanQuery.Builder uidBuilder = new BooleanQuery.Builder();
            uidBuilder.add(indexQuery.toQuery(context), BooleanClause.Occur.MUST);
            uidBuilder.add(idQuery.boost(boost).toQuery(context), BooleanClause.Occur.MUST);
            Query uidQuery = uidBuilder.build();
            builder.add(uidQuery, BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }


}
