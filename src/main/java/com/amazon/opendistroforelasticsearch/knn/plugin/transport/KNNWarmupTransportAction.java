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

package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexShard;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

public class KNNWarmupTransportAction extends TransportBroadcastByNodeAction<KNNWarmupRequest, KNNWarmupResponse,
        TransportBroadcastByNodeAction.EmptyResult> {

    public static Logger logger = LogManager.getLogger(KNNWarmupTransportAction.class);

    private IndicesService indicesService;

    @Inject
    public KNNWarmupTransportAction(ClusterService clusterService, TransportService transportService, IndicesService indicesService,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(KNNWarmupAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                KNNWarmupRequest::new, ThreadPool.Names.SEARCH);
        this.indicesService = indicesService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected KNNWarmupResponse newResponse(KNNWarmupRequest request, int totalShards, int successfulShards, int failedShards, List<EmptyResult> emptyResults, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new KNNWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected KNNWarmupRequest readRequestFrom(StreamInput in) throws IOException {
        return new KNNWarmupRequest(in);
    }

    @Override
    protected EmptyResult shardOperation(KNNWarmupRequest request, ShardRouting shardRouting) throws IOException {
        try {
            KNNIndexShard knnIndexShard = new KNNIndexShard(indicesService.indexServiceSafe(shardRouting.shardId()
                    .getIndex()).getShard(shardRouting.shardId().id()));
            knnIndexShard.warmup();
        } catch (KNNIndexShard.NotKNNIndexException e) {
            e.printStackTrace();
        }
        return EmptyResult.INSTANCE;
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, KNNWarmupRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, KNNWarmupRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, KNNWarmupRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
