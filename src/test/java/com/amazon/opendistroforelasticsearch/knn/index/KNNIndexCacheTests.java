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

package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.index.v2011.KNNIndex;
import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache.GRAPH_COUNT;

public class KNNIndexCacheTests extends ESSingleNodeTestCase {
    private final String testIndexName = "test_index";
    private final String testFieldName = "test_field";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(KNNPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        KNNTestCase.resetState();
    }

    public void testGetIndicesCacheStats() throws IOException, InterruptedException, ExecutionException {
        // Check that indiceCacheStats starts out at 0
        Map<String, Map<String, Object>> indiceCacheStats = KNNIndexCache.getInstance().getIndicesCacheStats();
        assertEquals(0, indiceCacheStats.size());

        // Create one KNN index
        String testIndexName1 = testIndexName + "1";
        createIndex(testIndexName1, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName1, testFieldName, 2);

        Long[] vector = {0L, 0L};
        addKnnDoc(testIndexName1, "1", testFieldName, vector);
        float[] queryVector = {0L, 0L};
        searchKNNIndex(testIndexName1, testFieldName, queryVector, 2);

        // Confirm that 1 index is added to IndicesCacheStats and 1 graph is added
        indiceCacheStats = KNNIndexCache.getInstance().getIndicesCacheStats();
        assertEquals(1L, indiceCacheStats.size());
        assertEquals(1, indiceCacheStats.get(testIndexName1).get(GRAPH_COUNT));

        // Create second KNN index
        String testIndexName2 = testIndexName + "2";
        createIndex(testIndexName2, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName2, testFieldName, 2);

        for (int i = 0; i < 3; i++) {
            addKnnDoc(testIndexName2, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }

        searchKNNIndex(testIndexName2, testFieldName, queryVector, 2);

        // Confirm that 2 indices are added in getIndicesCacheStats
        indiceCacheStats = KNNIndexCache.getInstance().getIndicesCacheStats();
        assertEquals(2L, indiceCacheStats.size());

        // Evict all of the graphs from testIndexName2 out of the cache
        for (String graphName : KNNIndexCache.getInstance().getGraphNamesForIndex(testIndexName2)) {
            KNNIndexCache.getInstance().evictGraphFromCache(graphName);
        }

        // Confirm that after all of the graphs of an index get evicted, there is only one index in the indiceCacheStats
        indiceCacheStats = KNNIndexCache.getInstance().getIndicesCacheStats();
        assertEquals(1L, indiceCacheStats.size());
    }

    public void testGetWeightInKilobytes() throws IOException, InterruptedException, ExecutionException {
        // Check that indiceCacheStats starts out at 0
        Map<String, Map<String, Object>> indiceCacheStats = KNNIndexCache.getInstance().getIndicesCacheStats();
        assertEquals(0, indiceCacheStats.size());

        // Assert total weight in cache is 0
        assertEquals((Long) 0L, KNNIndexCache.getInstance().getWeightInKilobytes());

        // Create 1 index
        String testIndexName1 = testIndexName + "1";
        createIndex(testIndexName1, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName1, testFieldName, 2);

        Long[] vector = {0L, 0L};
        for (int i = 0; i < 3; i++) {
            addKnnDoc(testIndexName1, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }

        float[] queryVector = {0L, 0L};
        searchKNNIndex(testIndexName1, testFieldName, queryVector, 2);

        // Confirm that that index's weight in cache is equal to the total weight in the cache
        assertTrue(KNNIndexCache.getInstance().getWeightInKilobytes() > 0L);
        assertEquals(KNNIndexCache.getInstance().getWeightInKilobytes(),
                KNNIndexCache.getInstance().getWeightInKilobytes(testIndexName1));

        // Add a second index
        String testIndexName2 = testIndexName + "2";
        createIndex(testIndexName2, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName2, testFieldName, 2);
        for (int i = 0; i < 3; i++) {
            addKnnDoc(testIndexName2, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }
        searchKNNIndex(testIndexName2, testFieldName, queryVector, 2);

        // Confirm that that index's weight in cache is greater than 0 and that the weights of testIndexName1 and
        // testIndexName2 add up to the total weight
        assertTrue(KNNIndexCache.getInstance().getWeightInKilobytes(testIndexName2) > 0L);
        assertEquals(KNNIndexCache.getInstance().getWeightInKilobytes(),
                (Long)(KNNIndexCache.getInstance().getWeightInKilobytes(testIndexName1) +
                        KNNIndexCache.getInstance().getWeightInKilobytes(testIndexName2)));
    }

    public void testGetGraphNames() throws IOException, InterruptedException, ExecutionException {
        // Create an index
        String testIndexName1 = testIndexName + "1";
        createIndex(testIndexName1, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName1, testFieldName, 2);

        Long[] vector = {0L, 0L};
        for (int i = 0; i < 5; i++) {
            addKnnDoc(testIndexName1, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }

        float[] queryVector = {0L, 0L};
        searchKNNIndex(testIndexName1, testFieldName, queryVector, 2);

        // Make sure it returns the correct names
        assertTrue(KNNIndexCache.getInstance().getGraphNamesForIndex(testIndexName1).size() > 0);
        for (String graphName : KNNIndexCache.getInstance().getGraphNamesForIndex(testIndexName1)) {
            assertTrue(graphName.contains("hnsw"));
        }
    }

    public void testEvictGraphs() throws IOException, InterruptedException, ExecutionException {
        // Create an index
        String testIndexName1 = testIndexName + "1";
        createIndex(testIndexName1, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName1, testFieldName, 2);

        Long[] vector = {0L, 0L};
        for (int i = 0; i < 3; i++) {
            addKnnDoc(testIndexName1, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }

        float[] queryVector = {0L, 0L};
        searchKNNIndex(testIndexName1, testFieldName, queryVector, 2);

        // Confirm that there is at least 1 graph in the cache for testIndexName1
        assertTrue((int) KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName1)
                .get(GRAPH_COUNT) > 0);

        // Evict all of the graphs for one index from the graph
        for (String graphName : KNNIndexCache.getInstance().getGraphNamesForIndex(testIndexName1)) {
            KNNIndexCache.getInstance().evictGraphFromCache(graphName);
        }
        assertFalse(KNNIndexCache.getInstance().getIndicesCacheStats().containsKey(testIndexName1));

        // add testIndexName1 back into the cache by searching
        searchKNNIndex(testIndexName1, testFieldName, queryVector, 2);

        // Create a second index
        String testIndexName2 = testIndexName + "2";
        createIndex(testIndexName2, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName2, testFieldName, 2);
        for (int i = 0; i < 3; i++) {
            addKnnDoc(testIndexName2, Integer.toString(i), testFieldName, vector);
            Thread.sleep(200);
        }
        searchKNNIndex(testIndexName2, testFieldName, queryVector, 2);

        // Confirm that the cache has at least 1 graph for each index
        assertTrue((int) KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName1)
                .get(GRAPH_COUNT) > 0);
        assertTrue((int) KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName2)
                .get(GRAPH_COUNT) > 0);

        // Evict all and make sure the graph is empty
        KNNIndexCache.getInstance().evictAllGraphsFromCache();
        assertEquals(0, KNNIndexCache.getInstance().getIndicesCacheStats().size());
    }

    public void testGetIndices() throws InterruptedException, ExecutionException, IOException {
        assertEquals(0, KNNIndexCache.getInstance().getIndicesCacheStats().size());

        IndexService indexService = createIndex(testIndexName, getKNNDefaultIndexSettings());
        createKnnIndexMapping(testIndexName, testFieldName, 2);

        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {1.0f, 2.0f});
        client().admin().indices().prepareFlush(testIndexName).execute();
        addKnnDoc(testIndexName, "2", testFieldName, new Float[] {1.0f, 2.0f});

        KNNIndexShard knnIndexShard = new KNNIndexShard(indexService.iterator().next());
        Engine.Searcher searcher = knnIndexShard.getIndexShard().acquireSearcher("test-cache");
        List<String> segmentPaths = knnIndexShard.getHNSWPaths(searcher.getIndexReader());

        List<KNNIndex> knnIndices = KNNIndexCache.getInstance().getIndices(segmentPaths, testIndexName);
        assertEquals(2, knnIndices.size());
        assertEquals(2, KNNIndexCache.getInstance().getIndicesCacheStats().get(testIndexName).get(GRAPH_COUNT));

        searcher.close();
    }

    protected void createKnnIndexMapping(String indexName, String fieldName, Integer dimensions) {
        PutMappingRequest request = new PutMappingRequest(indexName).type("_doc");
        request.source(fieldName, "type=knn_vector,dimension="+dimensions);
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());
    }

    protected Settings getKNNDefaultIndexSettings() {
        return Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn", true)
                .build();
    }

    protected void addKnnDoc(String index, String docId, String fieldName, Object[] vector)
            throws IOException, InterruptedException, ExecutionException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field(fieldName, vector)
                .endObject();
        IndexRequest indexRequest = new IndexRequest()
                .index(index)
                .id(docId)
                .source(builder)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        IndexResponse response = client().index(indexRequest).get();
        assertEquals(response.status(), RestStatus.CREATED);
    }

    protected void searchKNNIndex(String index, String fieldName, float[] vector, int k) {
        SearchResponse response = client().prepareSearch(index).setQuery(new KNNQueryBuilder(fieldName, vector, k))
                .get();
        assertEquals(response.status(), RestStatus.OK);
    }
}
