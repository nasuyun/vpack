package org.elasticsearch.apack.plain;

import org.elasticsearch.apack.plain.action.PlainSearchRequest;
import org.elasticsearch.apack.plain.util.CollectionUtils;
import org.elasticsearch.apack.plain.util.DateUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RestRequestReplacer {

    private static final String FIELD_TIME_ZONE = "time_zone";
    private static final String FIELD_COLUMNS = "columns";
    private static final String FIELD_FORMAT = "format";

    /**
     * 处理 RestRequest
     * 提取扩展参数构建扩展的 PlainSearchRequest
     * 转换删除扩展参数后的 RestRequest 提供给原始 SearchAction 查询
     *
     * @param request
     * @param searchActionRequestConsumer 提取扩展参数
     * @param replacedRestRequestConsumer 转换成原始的RestRequest
     * @throws IOException
     */
    public static void onReplace(
            RestRequest request,
            Consumer<PlainSearchRequest> searchActionRequestConsumer,
            Consumer<RestRequest> replacedRestRequestConsumer) throws IOException {
        request.withContentOrSourceParamParserOrNull(parser -> {
            Map<String, Object> params = parser == null ? Collections.emptyMap() : parser.map();
            searchActionRequestConsumer.accept(buildActionRequest(params));
            RestRequest filterRestRequest = filter(request);
            setParamsConsumed(request, filterRestRequest);
            replacedRestRequestConsumer.accept(filterRestRequest);
        });
    }

    /**
     * 构建扩展后的SearchRequest
     *
     * @param params params
     * @return PlainSearchRequest
     */
    private static PlainSearchRequest buildActionRequest(Map<String, Object> params) {
        PlainSearchRequest searchRequest = new PlainSearchRequest();
        String timezone = (String) params.get(FIELD_TIME_ZONE);
        String format = (String) params.get(FIELD_FORMAT);
        searchRequest.columns(parseColumns(params.get(FIELD_COLUMNS)));
        searchRequest.zoneId(Strings.isNullOrEmpty(timezone) ? DateUtils.UTC : ZoneId.of(timezone));
        searchRequest.format(Strings.isNullOrEmpty(format) ? TextFormat.fromMediaTypeOrFormat("txt") : TextFormat.fromMediaTypeOrFormat(format));
        return searchRequest;
    }

    private static String[] parseColumns(Object columnParam) {
        if (columnParam == null) {
            return Strings.EMPTY_ARRAY;
        }
        if (columnParam instanceof List) {
            List<String> columns = (List) columnParam;
            return columns.toArray(new String[columns.size()]);
        }
        if (columnParam instanceof String) {
            String columns = (String) columnParam;
            return columns.replaceAll("\\s+", "").split(",");
        }
        throw new PlainIllegalArgumentException("unkonw columns filed : " + columnParam);
    }

    /**
     * 删除扩展参数转换
     *
     * @param request
     * @return
     * @throws IOException
     */
    private static RestRequest filter(RestRequest request) throws IOException {
        RestRequestFilter filter = () -> Collections.unmodifiableSet(Sets.newHashSet(FIELD_TIME_ZONE, FIELD_COLUMNS, FIELD_FORMAT));
        return filter.getFilteredRequest(request);
    }

    private static void setParamsConsumed(RestRequest before, RestRequest after) {
        before.params().forEach((k, v) -> {
            before.param(k);
            after.param(k);
        });
    }

}