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

package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.KNNSingleNodeTestCase;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache.GRAPH_COUNT;
import static java.util.Collections.emptyList;


public class KNNIndexShardTests extends KNNSingleNodeTestCase {

    private final String testIndexName = "test-index";
    private final String testFieldName = "test-field";
    private final int dimensions = 2;

    public void testGetIndexShard() throws InterruptedException, ExecutionException, IOException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {2.5F, 3.5F});

        IndexShard indexShard = indexService.iterator().next();
        KNNIndexShard knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(indexShard, knnIndexShard.getIndexShard());
    }

    public void testGetIndexName() throws InterruptedException, ExecutionException, IOException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {2.5F, 3.5F});

        IndexShard indexShard = indexService.iterator().next();
        KNNIndexShard knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(testIndexName, knnIndexShard.getIndexName());
    }

    public void testWarmup_emptyIndex() throws IOException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);

        IndexShard indexShard = indexService.iterator().next();
        KNNIndexShard knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(emptyList(), knnIndexShard.warmup());
    }

    public void testWarmup_shardPresentInCache() throws InterruptedException, ExecutionException, IOException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {2.5F, 3.5F});

        searchKNNIndex(testIndexName, testFieldName, new float[] {1.0f, 2.0f}, 1);
        assertEquals(1, KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName).get(GRAPH_COUNT));

        IndexShard indexShard = indexService.iterator().next();
        KNNIndexShard knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(1, knnIndexShard.warmup().size());
        assertEquals(1, KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName).get(GRAPH_COUNT));
    }

    public void testWarmup_shardNotPresentInCache() throws InterruptedException, ExecutionException, IOException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        IndexShard indexShard;
        KNNIndexShard knnIndexShard;

        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {2.5F, 3.5F});
        client().admin().indices().prepareFlush(testIndexName).execute();

        indexShard = indexService.iterator().next();
        knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(1, knnIndexShard.warmup().size());
        assertEquals(1, KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName).get(GRAPH_COUNT));

        addKnnDoc(testIndexName, "2", testFieldName, new Float[] {2.5F, 3.5F});
        indexShard = indexService.iterator().next();
        knnIndexShard = new KNNIndexShard(indexShard);
        assertEquals(2, knnIndexShard.warmup().size());
        assertEquals(2, KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName).get(GRAPH_COUNT));
    }

    public void testGetHNSWPaths() throws IOException, ExecutionException, InterruptedException {
        IndexService indexService = createKNNIndex(testIndexName);
        createKnnIndexMapping(testIndexName, testFieldName, dimensions);
        IndexShard indexShard;
        KNNIndexShard knnIndexShard;
        Engine.Searcher searcher;
        List<String> hnswPaths;

        indexShard = indexService.iterator().next();
        knnIndexShard = new KNNIndexShard(indexShard);

        searcher = indexShard.acquireSearcher("test-hnsw-paths-1");
        hnswPaths = knnIndexShard.getHNSWPaths(searcher.getIndexReader());
        assertEquals(0, hnswPaths.size());
        searcher.close();

        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {2.5F, 3.5F});

        searcher = indexShard.acquireSearcher("test-hnsw-paths-2");
        hnswPaths = knnIndexShard.getHNSWPaths(searcher.getIndexReader());
        assertEquals(1, hnswPaths.size());
        assertTrue(hnswPaths.get(0).contains("hnsw") || hnswPaths.get(0).contains("hnswc"));
        searcher.close();
    }
}
