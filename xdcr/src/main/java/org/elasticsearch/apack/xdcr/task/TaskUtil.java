package org.elasticsearch.apack.xdcr.task;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;

import java.util.*;

import static org.elasticsearch.apack.xdcr.task.TaskIds.repositoryIndex;

public class TaskUtil {

    /**
     * 返回已注册的同步索引任务
     *
     * @param state 状态
     * @return Map Key:远程集群名 Value: 索引列表
     */
    public static Map<String, List<String>> syncMap(ClusterState state) {
        Map<String, List<String>> result = new HashMap<>();
        Map<String, PersistentTasksCustomMetaData.PersistentTask<?>> taskMap = PersistentTasksCustomMetaData
                .builder(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE)).build().taskMap();

        taskMap.forEach((k, v) -> {
            if (k.startsWith(TaskIds.SYNC_INDEX_TASKNAME_PREFIX)) {
                Tuple<String, String> repositoryIndex = repositoryIndex(k);
                List<String> indices = result.computeIfAbsent(repositoryIndex.v1(), (s) -> new ArrayList<>());
                indices.add(repositoryIndex.v2());
            }
        });
        return result;
    }

    public static List<String> indices(ClusterState state, String repository) {
        Map<String, List<String>> map = syncMap(state);
        return map.containsKey(repository) ? map.get(repository) : Collections.emptyList();
    }
}
