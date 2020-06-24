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

import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupRequest;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import java.util.List;

/**
 * This handler is responsible for processing Warmup API RestRequests and converting them to transport requests. From
 * the rest request, an array of indices should be extracted and passed to all nodes through the transport layer
 */
public class RestKNNWarmupHandler extends BaseRestHandler {
    public static String NAME = "knn_warmup_action";



    public RestKNNWarmupHandler(Settings settings, RestController controller) {}

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
        return channel -> client.execute(KNNWarmupAction.INSTANCE, knnWarmupRequest, new RestActions.NodesResponseRestListener<>(channel));
    }

    /**
     * Take a RestRequest and converts it into KNNWarmupRequest
     *
     * @param request RestRequest for warmup API
     * @return Transport request for warmup API
     */
    private KNNWarmupRequest createKNNWarmupRequest(RestRequest request) {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        return new KNNWarmupRequest(indices);
    }
}