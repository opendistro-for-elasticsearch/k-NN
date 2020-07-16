/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.knn.common.exception.KNNInvalidIndicesException;
import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupRequest;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.KNN_INDEX;
import static org.elasticsearch.action.support.IndicesOptions.strictExpandOpen;

/**
 * RestHandler for k-NN index warmup API. API provides the ability for a user to load specific indices' k-NN graphs
 * into memory.
 */
public class RestKNNWarmupHandler extends BaseRestHandler {
    public static String NAME = "knn_warmup_action";

    private static final Logger logger = LogManager.getLogger(RestKNNWarmupHandler.class);

    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ClusterService clusterService;

    public RestKNNWarmupHandler(Settings settings, RestController controller, ClusterService clusterService,
                                IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
                new Route(RestRequest.Method.GET, KNNPlugin.KNN_BASE_URI + "/warmup/{index}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        KNNWarmupRequest knnWarmupRequest = createKNNWarmupRequest(request);
        logger.info("[KNN] Warmup started for the following indices: "
                + String.join(",", knnWarmupRequest.indices()));
        return channel -> client.execute(KNNWarmupAction.INSTANCE, knnWarmupRequest, new RestToXContentListener<>(channel));
    }

    private KNNWarmupRequest createKNNWarmupRequest(RestRequest request) {
        String[] indexNames = Strings.splitStringByCommaToArray(request.param("index"));
        Index[] indices =  indexNameExpressionResolver.concreteIndices(clusterService.state(), strictExpandOpen(),
                indexNames);
        List<String> invalidIndexNames = new ArrayList<>();

        Arrays.stream(indices).forEach(index -> {
            if (!"true".equals(clusterService.state().metadata().getIndexSafe(index).getSettings().get(KNN_INDEX))) {
                invalidIndexNames.add(index.getName());
            }
        });

        if (invalidIndexNames.size() != 0) {
            throw new KNNInvalidIndicesException(invalidIndexNames,
                    "Warm up request rejected. One or more indices have 'index.knn' set to false.");
        }

        return new KNNWarmupRequest(indexNames);
    }
}
