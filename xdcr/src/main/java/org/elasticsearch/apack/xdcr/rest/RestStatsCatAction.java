/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.apack.xdcr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.apack.xdcr.action.stats.Stats;
import org.elasticsearch.apack.xdcr.action.stats.StatsAction;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestStatsCatAction extends AbstractCatAction {
    public RestStatsCatAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_cat/xdcr", this);
    }

    @Override
    public String getName() {
        return "xdcr_cat_stats_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {

        return channel -> client.execute(StatsAction.INSTANCE, new StatsAction.Request(), new RestResponseListener<StatsAction.Response>(channel) {
            @Override
            public RestResponse buildResponse(StatsAction.Response response) throws Exception {
                Table tab = buildTable(request, response);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/xdcr\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("repository", "alias:r;desc:repository");
        table.addCell("index", "alias:i,idx;desc:index alias points to");
        table.addCell("shard", "alias:s;desc:index shard");
        table.addCell("localMaxSeqNo", "alias:ls;desc:local maxSeqNo");
        table.addCell("remoteMaxSeqNo", "alias:rs;desc:remote maxSeqNo");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, StatsAction.Response response) {
        Table table = getTableWithHeader(request);
        for (Stats stats : response.stats()) {
            table.startRow();
            table.addCell(stats.repository);
            table.addCell(stats.index);
            table.addCell(stats.shard);
            table.addCell(stats.localSeqNoStats.getMaxSeqNo());
            table.addCell(stats.remoteSeqNoStats.getMaxSeqNo());
            table.endRow();
        }
        return table;
    }

}
