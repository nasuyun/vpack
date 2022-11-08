/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.apack;

import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.apack.rank.RankActionFilter;
import org.elasticsearch.apack.rank.RankQueryBuilder;

import java.util.List;

import static java.util.Arrays.asList;


public class RankPlugin extends Plugin implements SearchPlugin, ActionPlugin {

    @Override
    public List<ActionFilter> getActionFilters() {
        return asList(new RankActionFilter());
    }

    @Override
    public List<SearchPlugin.QuerySpec<?>> getQueries() {
        return asList(new SearchPlugin.QuerySpec<>(RankQueryBuilder.NAME, RankQueryBuilder::new, RankQueryBuilder::fromXContent));
    }

}
