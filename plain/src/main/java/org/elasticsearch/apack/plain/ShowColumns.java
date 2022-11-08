package org.elasticsearch.apack.plain;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.apack.plain.schema.expression.Attribute;
import org.elasticsearch.apack.plain.schema.expression.FieldAttribute;
import org.elasticsearch.apack.plain.schema.resovler.IndexResolver;
import org.elasticsearch.apack.plain.type.EsField;
import org.elasticsearch.apack.plain.type.KeywordEsField;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.*;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.apack.plain.type.DataType.OBJECT;

public class ShowColumns {

    final IndexResolver indexResolver;
    final IndexNameExpressionResolver indexNameExpressionResolver;
    final ClusterService clusterService;

    public ShowColumns(IndexResolver indexResolver, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService) {
        this.indexResolver = indexResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
    }

    public void onSchema(SearchRequest searchRequest, String[] requiredColumns, ActionListener<List<Attribute>> listener) {
        final Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterService.state(), searchRequest.indices());
        String[] indices = indicesAndAliases.toArray(new String[indicesAndAliases.size()]);
        indexResolver.resolveAsMergedMapping(indices, wrap(
                indexResolution -> {
                    List<Attribute> mappingFields = flatten(indexResolution.get().mapping(), null);
                    List<Attribute> fields = resolvedFields(mappingFields, requiredColumns);
                    listener.onResponse(fields);
                },
                listener::onFailure
        ));
    }

    private static List<Attribute> resolvedFields(List<Attribute> extraMappingAttribues, String[] requiredColumns) {
        if (requiredColumns == null || requiredColumns.length == 0) {
            return extraMappingAttribues;
        }
        // 用户选择字段
        Map<String, Attribute> extraMappingAttribuesToMap = extraMappingAttribuesToMap(extraMappingAttribues);
        List<Attribute> resovledFields = new ArrayList<>();
        List<String> requiredNomalColumn = new ArrayList<>();
        for (String requiredColumn : requiredColumns) {
            if (innerFields().containsKey(requiredColumn)) {
                resovledFields.add(innerFields().get(requiredColumn));
            } else {
                requiredNomalColumn.add(requiredColumn);
            }
        }
        if (requiredNomalColumn.isEmpty()) {
            resovledFields.addAll(extraMappingAttribues);
        } else {
            for (String s : requiredNomalColumn) {
                if (extraMappingAttribuesToMap.containsKey(s)) {
                    resovledFields.add(extraMappingAttribuesToMap.get(s));
                } else {
                    throw new IllegalArgumentException("column not found : " + s);
                }
            }
        }
        return resovledFields;
    }

    private static Map<String, Attribute> extraMappingAttribuesToMap(List<Attribute> attributes) {
        Map<String, Attribute> map = new LinkedHashMap<>();
        for (Attribute attribute : attributes) {
            map.put(attribute.name(), attribute);
        }
        return map;
    }

    // 系统内置字段
    private static Map<String, Attribute> innerFields() {
        Map<String, Attribute> inners = new HashMap<>();
        inners.put("_index", new FieldAttribute("_index", new KeywordEsField("_index")));
        inners.put("_id", new FieldAttribute("_id", new KeywordEsField("_id")));
        inners.put("_type", new FieldAttribute("_type", new KeywordEsField("_type")));
        inners.put("_score", new FieldAttribute("_score", new KeywordEsField("_score")));
        return inners;
    }

    private static List<Attribute> flatten(Map<String, EsField> mapping, FieldAttribute parent) {
        List<Attribute> list = new ArrayList<>();
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField esField = entry.getValue();

            if (esField != null) {
                FieldAttribute f = new FieldAttribute(parent, parent != null ? parent.name() + "." + name : name, esField);
                if (OBJECT.equals(f.dataType()) == false) {
                    list.add(f);
                }
                // object or nested
                if (esField.getProperties().isEmpty() == false) {
                    list.addAll(flatten(esField.getProperties(), f));
                }
            }
        }
        return list;
    }

}
