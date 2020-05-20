/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.plugin.rest;

import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsRequest;

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStats;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Resthandler for stats api endpoint. The user has the ability to get all stats from
 * all nodes or select stats from specific nodes.
 */
public class RestKNNStatsHandler extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(RestKNNStatsHandler.class);
    private static final String NAME = "knn_stats_action";
    private KNNStats knnStats;

    /**
     * Constructor
     *
     * @param settings Settings
     * @param controller Rest Controller
     * @param knnStats KNNStats
     */
    public RestKNNStatsHandler(Settings settings, RestController controller, KNNStats knnStats) {
        this.knnStats = knnStats;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
                new Route(RestRequest.Method.GET, KNNPlugin.KNN_BASE_URI + "/{nodeId}/stats/"),
                new Route(RestRequest.Method.GET, KNNPlugin.KNN_BASE_URI + "/{nodeId}/stats/{stat}"),
                new Route(RestRequest.Method.GET, KNNPlugin.KNN_BASE_URI + "/stats/"),
                new Route(RestRequest.Method.GET, KNNPlugin.KNN_BASE_URI + "/stats/{stat}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        // From restrequest, create a knnStatsRequest
        KNNStatsRequest knnStatsRequest = getRequest(request);

        return channel -> client.execute(KNNStatsAction.INSTANCE, knnStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
    }

    /**
     * Creates a KNNStatsRequest from a RestRequest
     *
     * @param request Rest request
     * @return KNNStatsRequest
     */
    private KNNStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query
        String[] nodeIdsArr = null;
        String nodesIdsStr = request.param("nodeId");
        if (!Strings.isEmpty(nodesIdsStr)) {
            nodeIdsArr = nodesIdsStr.split(",");
        }

        KNNStatsRequest knnStatsRequest = new KNNStatsRequest(knnStats.getStats().keySet(), nodeIdsArr);
        knnStatsRequest.timeout(request.param("timeout"));

        // parse the stats the customer wants to see
        Set<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            knnStatsRequest.all();
        } else if (statsSet.size() == 1 && statsSet.contains("_all")) {
            knnStatsRequest.all();
        } else if (statsSet.contains(KNNStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException("Request " + request.path() + " contains _all and individual stats");
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (!knnStatsRequest.addStat(stat)) {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats,
                        knnStatsRequest.getStatsToBeRetrieved(), "stat"));
            }

        }
        return knnStatsRequest;
    }
}
