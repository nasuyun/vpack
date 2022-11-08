package org.elasticsearch.apack.xdcr.utils;

import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.RemoteConnectionStrategy;

public class RemoteConnections {

    /**
     * 远程连接是否开启
     *
     * @param repository repository
     * @param settings   settings
     * @return isConnectionEnabled
     */
    public static boolean isConnectionEnabled(String repository, Settings settings) {
        return RemoteConnectionStrategy.isConnectionEnabled(repository, settings);
    }

    /**
     * 远程集群是否已连接
     *
     * @param client     client
     * @param repository repository
     * @return isConnected
     */
    public static boolean isConnected(Client client, String repository) {
        try {
            MainResponse response = client.getRemoteClusterClient(repository)
                    .execute(MainAction.INSTANCE, new MainRequest()).actionGet(TimeValue.timeValueSeconds(5));
            return response.isAvailable();
        } catch (Exception e) {
            return false;
        }
    }
}
