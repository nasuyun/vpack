package org.elasticsearch.apack.xdcr.utils;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.*;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.indices.IndicesRequestCache;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.elasticsearch.apack.XDCRSettings.XDCR_FOLLOWING_INDEX_SETTING;


public class SettingUtils {

    static final Set<Setting<?>> NON_REPLICATED_SETTINGS;

    /**
     * 同步索引配置白名单
     */
    static {
        final Set<Setting<?>> nonReplicatedSettings = new HashSet<>();
        // whiteListedSettings.add(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING);

        // 同步路由配置
        // nonReplicatedSettings.add(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING);
        // nonReplicatedSettings.add(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING);
        // nonReplicatedSettings.add(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING);

        nonReplicatedSettings.add(IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_READ_ONLY_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_BLOCKS_READ_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_BLOCKS_METADATA_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING);
        nonReplicatedSettings.add(IndexMetaData.INDEX_PRIORITY_SETTING);
        nonReplicatedSettings.add(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS);

        nonReplicatedSettings.add(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING);
        nonReplicatedSettings.add(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        nonReplicatedSettings.add(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING);
        nonReplicatedSettings.add(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY);
        nonReplicatedSettings.add(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING);

        nonReplicatedSettings.add(IndexSettings.MAX_RESULT_WINDOW_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_WARMER_ENABLED_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_RESCORE_WINDOW_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING);
        nonReplicatedSettings.add(IndexSettings.DEFAULT_FIELD_SETTING);
        nonReplicatedSettings.add(IndexSettings.QUERY_STRING_LENIENT_SETTING);
        nonReplicatedSettings.add(IndexSettings.QUERY_STRING_ANALYZE_WILDCARD);
        nonReplicatedSettings.add(IndexSettings.QUERY_STRING_ALLOW_LEADING_WILDCARD);
        nonReplicatedSettings.add(IndexSettings.ALLOW_UNMAPPED);
        nonReplicatedSettings.add(IndexSettings.MAX_SCRIPT_FIELDS_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_REGEX_LENGTH_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_TERMS_COUNT_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_ANALYZED_OFFSET_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_SLICES_PER_SCROLL);
        nonReplicatedSettings.add(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_GC_DELETES_SETTING);
        nonReplicatedSettings.add(IndexSettings.INDEX_TTL_DISABLE_PURGE_SETTING);
        nonReplicatedSettings.add(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD);

        nonReplicatedSettings.add(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING);
        nonReplicatedSettings.add(BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING);

        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING);
        nonReplicatedSettings.add(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING);
        nonReplicatedSettings.add(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING);

        nonReplicatedSettings.add(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING);
        nonReplicatedSettings.add(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING);

        nonReplicatedSettings.add(MergeSchedulerConfig.AUTO_THROTTLE_SETTING);
        nonReplicatedSettings.add(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING);
        nonReplicatedSettings.add(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING);
        nonReplicatedSettings.add(EngineConfig.INDEX_CODEC_SETTING);

        NON_REPLICATED_SETTINGS = Collections.unmodifiableSet(nonReplicatedSettings);
    }

    public static Settings filter(Settings originalSettings) {
        Settings.Builder settings = Settings.builder().put(originalSettings);
        settings.remove(XDCR_FOLLOWING_INDEX_SETTING.getKey());
        settings.remove(IndexMetaData.SETTING_INDEX_UUID);
        settings.remove(IndexMetaData.SETTING_VERSION_CREATED);
        settings.remove(IndexMetaData.SETTING_INDEX_PROVIDED_NAME);
        settings.remove(IndexMetaData.SETTING_CREATION_DATE);
        settings.remove(IndexMetaData.SETTING_VERSION_UPGRADED);
        settings.remove(IndexMetaData.SETTING_VERSION_UPGRADED_STRING);

        Iterator<String> iterator = settings.keys().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            for (Setting<?> whitelistedSetting : NON_REPLICATED_SETTINGS) {
                if (whitelistedSetting.match(key)) {
                    iterator.remove();
                    break;
                }
            }
        }
        return settings.build();
    }
}
