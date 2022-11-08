/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.apack;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CombinedRateLimiter;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public final class XDCRSettings {

    /**
     * 线程池配置
     */
    public static final String XDCR_THREAD_POOL_MEATADATA_COORDINATE = "xdcr_metadata_coordinate";
    public static final String XDCR_THREAD_POOL_CLUSTER_COORDINATE = "xdcr_cluster_coordinate";
    public static final String XDCR_THREAD_POOL_BULK = "xdcr_shard_bulk";

    public static final String XDCR_CUSTOM_METADATA_KEY = "xdcr";
    public static final String XDCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS = "leader_index_shard_history_uuids";

    public static final Setting<Boolean> XDCR_ENABLED_SETTING = Setting.boolSetting("apack.xdcr.enabled", true, Property.NodeScope);

    public static final Setting<TimeValue> CLUSTER_LEVEL_COORDINATE_INTERVAL_SETTING =
            Setting.timeSetting("xdcr.cluster_level_coordinate_interval", TimeValue.timeValueSeconds(60), Property.NodeScope, Property.Dynamic);

    public static final Setting<Boolean> XDCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.apack.xdcr.follower", false, Property.IndexScope, Property.InternalIndex);

    public static final Setting<String> XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING =
            Setting.simpleString("index.apack.xdcr.leader_index_uuid", Property.IndexScope, Property.InternalIndex);

    public static final Setting<String> XDCR_CUSTOM_METADATA_LEADER_INDEX_NAME_SETTING =
            Setting.simpleString("index.apack.xdcr.leader_index_name", Property.IndexScope, Property.InternalIndex);

    public static final Setting<String> XDCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_SETTING =
            Setting.simpleString("index.apack.xdcr.remote_cluster_name", Property.IndexScope, Property.InternalIndex);

    public static final Setting<TimeValue> XDCR_WAIT_FOR_METADATA_TIMEOUT_SETTING =
            Setting.timeSetting("xdcr.wait_for_metadata_timeout", TimeValue.timeValueSeconds(60), Property.NodeScope, Property.Dynamic);

    /**
     * 单个Shard索引文件写入限流(Restore FileChunk)
     */
    public static final Setting<ByteSizeValue> RECOVERY_MAX_BYTES_PER_SECOND_SETTING =
            Setting.byteSizeSetting("xdcr.indices.recovery.max_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB),
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * 一阶段恢复索引文件Chunk大小
     */
    public static final Setting<ByteSizeValue> RECOVERY_CHUNK_SIZE_SETTING =
            Setting.byteSizeSetting("xdcr.indices.recovery.chunk_size", new ByteSizeValue(5, ByteSizeUnit.MB),
                    new ByteSizeValue(1, ByteSizeUnit.KB), new ByteSizeValue(1, ByteSizeUnit.GB), Setting.Property.Dynamic,
                    Setting.Property.NodeScope);

    /**
     * 一阶段恢复最大文件数
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING =
            Setting.intSetting("xdcr.indices.recovery.max_concurrent_file_chunks", 5, 1, 10, Property.Dynamic, Property.NodeScope);

    /**
     * 一阶段恢复超时时间
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING =
            Setting.timeSetting("xdcr.indices.recovery.recovery_activity_timeout", TimeValue.timeValueSeconds(60),
                    Setting.Property.Dynamic, Setting.Property.NodeScope);
    /**
     * 一阶段恢复内部单个请求超时时间
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTION_TIMEOUT_SETTING =
            Setting.positiveTimeSetting("xdcr.indices.recovery.internal_action_timeout", TimeValue.timeValueSeconds(60),
                    Property.Dynamic, Property.NodeScope);

    /**
     * Shard同步未发现新数据时的等待时间
     */
    public static final Setting<TimeValue> INDICES_PULLING_IDEL_WAIT_SETTING = Setting.timeSetting(
            "xdcr.indices.pull.idel_wait_time", TimeValue.timeValueSeconds(60), Property.NodeScope, Property.Dynamic);


    public static List<Setting<?>> getSettings() {
        return Arrays.asList(
                XDCR_ENABLED_SETTING,
                XDCR_FOLLOWING_INDEX_SETTING,
                XDCR_CUSTOM_METADATA_LEADER_INDEX_UUID_SETTING,
                XDCR_CUSTOM_METADATA_LEADER_INDEX_NAME_SETTING,
                XDCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_SETTING,
                RECOVERY_MAX_BYTES_PER_SECOND_SETTING,
                INDICES_RECOVERY_ACTION_TIMEOUT_SETTING,
                INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING,
                INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                RECOVERY_CHUNK_SIZE_SETTING,
                XDCR_WAIT_FOR_METADATA_TIMEOUT_SETTING,
                CLUSTER_LEVEL_COORDINATE_INTERVAL_SETTING,
                INDICES_PULLING_IDEL_WAIT_SETTING);
    }

    private final CombinedRateLimiter recoveryRateLimiter;
    private volatile TimeValue recoveryActivityTimeout;
    private volatile TimeValue recoveryActionTimeout;
    private volatile ByteSizeValue chunkSize;
    private volatile int maxConcurrentFileChunks;
    private volatile TimeValue clusterLevelCoordinateInterval;

    private SetOnce<Consumer<TimeValue>> clusterLevelCoordinateIntervalConsumer = new SetOnce<>();

    public void setClusterLevelCoordinateIntervalConsumer(Consumer<TimeValue> intervalUpdater) {
        if (clusterLevelCoordinateIntervalConsumer.get() == null) {
            clusterLevelCoordinateIntervalConsumer.set(intervalUpdater);
        }
    }

    public XDCRSettings(Settings settings, ClusterSettings clusterSettings) {
        this.recoveryActivityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.recoveryActionTimeout = INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.get(settings);
        this.recoveryRateLimiter = new CombinedRateLimiter(RECOVERY_MAX_BYTES_PER_SECOND_SETTING.get(settings));
        this.chunkSize = RECOVERY_CHUNK_SIZE_SETTING.get(settings);
        this.maxConcurrentFileChunks = INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        this.clusterLevelCoordinateInterval = CLUSTER_LEVEL_COORDINATE_INTERVAL_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(RECOVERY_MAX_BYTES_PER_SECOND_SETTING, this::setMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(RECOVERY_CHUNK_SIZE_SETTING, this::setChunkSize);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setRecoveryActivityTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTION_TIMEOUT_SETTING, this::setRecoveryActionTimeout);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_LEVEL_COORDINATE_INTERVAL_SETTING, this::setClusterLevelCoordinateInterval);
    }

    private void setChunkSize(ByteSizeValue chunkSize) {
        this.chunkSize = chunkSize;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    private void setMaxBytesPerSec(ByteSizeValue maxBytesPerSec) {
        recoveryRateLimiter.setMBPerSec(maxBytesPerSec);
    }

    private void setRecoveryActivityTimeout(TimeValue recoveryActivityTimeout) {
        this.recoveryActivityTimeout = recoveryActivityTimeout;
    }

    private void setRecoveryActionTimeout(TimeValue recoveryActionTimeout) {
        this.recoveryActionTimeout = recoveryActionTimeout;
    }

    private void setClusterLevelCoordinateInterval(TimeValue timeValue) {
        this.clusterLevelCoordinateInterval = timeValue;
        if (clusterLevelCoordinateIntervalConsumer.get() != null) {
            clusterLevelCoordinateIntervalConsumer.get().accept(timeValue);
        }
    }

    public ByteSizeValue getChunkSize() {
        return chunkSize;
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    public CombinedRateLimiter getRecoveryRateLimiter() {
        return recoveryRateLimiter;
    }

    public TimeValue getRecoveryActivityTimeout() {
        return recoveryActivityTimeout;
    }

    public TimeValue getRecoveryActionTimeout() {
        return recoveryActionTimeout;
    }

    public TimeValue getClusterLevelCoordinateInterval() {
        return clusterLevelCoordinateInterval;
    }

}
