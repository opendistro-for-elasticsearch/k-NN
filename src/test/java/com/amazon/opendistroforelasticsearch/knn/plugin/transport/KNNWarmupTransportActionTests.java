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

import com.amazon.opendistroforelasticsearch.knn.KNNSingleNodeTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNWarmupTransportActionTests extends KNNSingleNodeTestCase {
    private final String testIndexName = "test-index";
    private final String testFieldName = "test-field";
    private final int dimensions = 2;

    public void testShardOperation() throws IOException, ExecutionException, InterruptedException {
        KNNWarmupRequest knnWarmupRequest = new KNNWarmupRequest(testIndexName);
        IndexService indexService;
        ShardRouting shardRouting;
        KNNWarmupTransportAction knnWarmupTransportAction = node().injector().getInstance(KNNWarmupTransportAction.class);
        assertEquals(0, KNNIndexCache.getInstance().getIndicesCacheStats().size());

        indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        shardRouting = indexService.iterator().next().routingEntry();

        knnWarmupTransportAction.shardOperation(knnWarmupRequest, shardRouting);
        assertEquals(0, KNNIndexCache.getInstance().getIndicesCacheStats().size());

        addKnnDoc(testIndexName, "1", testFieldName, new Long[] {0L, 1L});

        knnWarmupTransportAction.shardOperation(knnWarmupRequest, shardRouting);
        assertEquals(1, KNNIndexCache.getInstance().getIndicesCacheStats().size());
    }

    public void testShards() throws InterruptedException, ExecutionException, IOException {
        ClusterService clusterService = node().injector().getInstance(ClusterService.class);
        KNNWarmupTransportAction knnWarmupTransportAction = node().injector().getInstance(KNNWarmupTransportAction.class);
        KNNWarmupRequest knnWarmupRequest = new KNNWarmupRequest(testIndexName);

        createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        addKnnDoc(testIndexName, "1", testFieldName, new Long[] {0L, 1L});

        ShardsIterator shardsIterator = knnWarmupTransportAction.shards(clusterService.state(), knnWarmupRequest,
                new String[] {testIndexName});
        assertEquals(1, shardsIterator.size());
    }

    public void testCheckGlobalBlock() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterBlock metaReadClusterBlock = new ClusterBlock(randomInt(), "test-meta-data-block",
                false, false, false, RestStatus.FORBIDDEN,
                EnumSet.of(ClusterBlockLevel.METADATA_READ));
        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addGlobalBlock(metaReadClusterBlock).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).blocks(clusterBlocks).build();
        when(clusterService.state()).thenReturn(state);

        KNNWarmupTransportAction knnWarmupTransportAction = node().injector().getInstance(KNNWarmupTransportAction.class);
        KNNWarmupRequest knnWarmupRequest = new KNNWarmupRequest(testIndexName);
        assertNotNull(knnWarmupTransportAction.checkGlobalBlock(clusterService.state(), knnWarmupRequest));
    }

    public void testCheckRequestBlock() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterBlock metaReadClusterBlock = new ClusterBlock(randomInt(), "test-meta-data-block",
                false, false, false, RestStatus.FORBIDDEN,
                EnumSet.of(ClusterBlockLevel.METADATA_READ));
        ClusterBlocks clusterBlocks = ClusterBlocks.builder().addGlobalBlock(metaReadClusterBlock).build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).blocks(clusterBlocks).build();
        when(clusterService.state()).thenReturn(state);

        KNNWarmupTransportAction knnWarmupTransportAction = node().injector().getInstance(KNNWarmupTransportAction.class);
        KNNWarmupRequest knnWarmupRequest = new KNNWarmupRequest(testIndexName);
        assertNotNull(knnWarmupTransportAction.checkRequestBlock(clusterService.state(), knnWarmupRequest,
                new String[] {testIndexName}));
    }
}
