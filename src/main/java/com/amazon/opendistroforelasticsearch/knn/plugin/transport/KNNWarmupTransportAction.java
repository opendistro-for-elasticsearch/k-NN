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

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;
import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexShard;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class KNNWarmupTransportAction extends TransportNodesAction<KNNWarmupRequest, KNNWarmupResponse,
        KNNWarmupNodeRequest, KNNWarmupNodeResponse> {

    public static Logger logger = LogManager.getLogger(KNNWarmupTransportAction.class);

    private IndicesService indicesService;
    private NodeEnvironment nodeEnvironment;

    @Inject
    public KNNWarmupTransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            IndicesService indicesService,
            NodeEnvironment nodeEnvironment
    ) {
        super(KNNWarmupAction.NAME, threadPool, clusterService, transportService, actionFilters, KNNWarmupRequest::new,
                KNNWarmupNodeRequest::new, ThreadPool.Names.MANAGEMENT, KNNWarmupNodeResponse.class);
        this.indicesService = indicesService;
        this.nodeEnvironment = nodeEnvironment;
    }

    @Override
    protected KNNWarmupResponse newResponse(KNNWarmupRequest request, List<KNNWarmupNodeResponse> responses,
                                           List<FailedNodeException> failures) {
        return new KNNWarmupResponse(
                clusterService.getClusterName(),
                responses,
                failures
        );
    }

    @Override
    protected KNNWarmupNodeRequest newNodeRequest(KNNWarmupRequest request) {
        return new KNNWarmupNodeRequest(request.getIndices());
    }

    @Override
    protected KNNWarmupNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new KNNWarmupNodeResponse(in);
    }

    @Override
    protected KNNWarmupNodeResponse nodeOperation(KNNWarmupNodeRequest request) {
        AtomicInteger failureCount = new AtomicInteger(0);
        int graphCount = Arrays.stream(request.getIndices())
                .map(indexName -> clusterService.state().getMetaData().getIndices().get(indexName))
                .filter(Objects::nonNull)
                .filter(index -> indicesService.hasIndex(index.getIndex()))
                .map(index -> indicesService.indexServiceSafe(index.getIndex()).iterator())
                .map(indexShardIterator ->
                        StreamSupport.stream(Spliterators.spliteratorUnknownSize(indexShardIterator, Spliterator.ORDERED), false)
                                .map(indexShard -> {
                                    try {
                                        return new KNNIndexShard(indexShard, nodeEnvironment);
                                    } catch (KNNIndexShard.NotKNNIndexException e) {
                                        e.printStackTrace();
                                        failureCount.getAndIncrement();
                                        return null;
                                    }
                                })
                                .filter(Objects::nonNull)
                                .map(knnIndexShard -> {
                                    try {
                                        return KNNIndexCache.getInstance().loadIndex(knnIndexShard);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                        failureCount.getAndIncrement();
                                        return 0;
                                    }
                                })
                                .mapToInt(Integer::intValue)
                                .sum()
                )
                .mapToInt(Integer::intValue).sum();

        return new KNNWarmupNodeResponse(clusterService.localNode(), graphCount, failureCount.get());
    }
}
