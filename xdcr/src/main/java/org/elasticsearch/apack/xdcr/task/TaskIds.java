package org.elasticsearch.apack.xdcr.task;

import org.elasticsearch.common.collect.Tuple;

public class TaskIds {

    public final static String SYNC_METADATA_TASKNAME = "[xdcr-metadata-sync]";
    public final static String SYNC_CLUSTER_TASKNAME_PREFIX = "[xdcr-cluster-sync]";
    public final static String SYNC_INDEX_TASKNAME_PREFIX = "[xdcr-index-sync]";

    public static String syncClusterTaskName(String repo) {
        return SYNC_CLUSTER_TASKNAME_PREFIX + repo;
    }

    public static String indexSyncTaskName(String repository, String index) {
        return SYNC_INDEX_TASKNAME_PREFIX + repository + ":" + index;
    }

    public static String syncMetaTaskName() {
        return SYNC_METADATA_TASKNAME;
    }

    public static Tuple<String, String> repositoryIndex(String indexSyncTask) {
        String[] vv = indexSyncTask.substring(SYNC_INDEX_TASKNAME_PREFIX.length()).split(":");
        return Tuple.tuple(vv[0], vv[1]);
    }
}
