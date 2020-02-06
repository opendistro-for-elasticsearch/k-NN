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

package com.amazon.opendistroforelasticsearch.knn.plugin.action;

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;
import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStat;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStats;

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.StatNames;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNCacheSupplier;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNCircuitBreakerSupplier;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNInnerCacheStatsSupplier;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.rules.DisableOnDebug;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Before;

/**
 * Integration tests to check the correctness of RestKNNStatsHandler
 */
public class RestKNNStatsHandlerIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(RestKNNStatsHandlerIT.class);
    private boolean isDebuggingTest = new DisableOnDebug(null).isDebugging();
    private boolean isDebuggingRemoteCluster = System.getProperty("cluster.debug", "false").equals("true");

    private KNNStats knnStats;

    @Before
    public void setup() {
        Map<String, KNNStat<?>> stats = ImmutableMap.<String, KNNStat<?>>builder()
            .put(StatNames.HIT_COUNT.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::hitCount)))
            .put(StatNames.MISS_COUNT.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::missCount)))
            .put(StatNames.LOAD_SUCCESS_COUNT.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::loadSuccessCount)))
            .put(StatNames.LOAD_EXCEPTION_COUNT.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::loadExceptionCount)))
            .put(StatNames.TOTAL_LOAD_TIME.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::totalLoadTime)))
            .put(StatNames.EVICTION_COUNT.getName(), new KNNStat<>(false,
                    new KNNInnerCacheStatsSupplier(CacheStats::evictionCount)))
            .put(StatNames.GRAPH_MEMORY_USAGE.getName(), new KNNStat<>(false,
                    new KNNCacheSupplier<>(KNNIndexCache::getWeightInKilobytes)))
            .put(StatNames.CACHE_CAPACITY_REACHED.getName(), new KNNStat<>(false,
                    new KNNCacheSupplier<>(KNNIndexCache::isCacheCapacityReached)))
            .put(StatNames.CIRCUIT_BREAKER_TRIGGERED.getName(), new KNNStat<>(true,
                    new KNNCircuitBreakerSupplier())).build();

        knnStats = new KNNStats(stats);
    }

    /**
     *  Test checks that handler correctly returns all metrics
     *  @throws IOException throws IOException
     */
    public void testCorrectStatsReturned() throws IOException {

        Request statsRequest = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/stats"
        );

        // Check that all of the cluster level metrics are returned
        String statsResponseBody = makeRequestAndReturnResponseBody(statsRequest);
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), statsResponseBody).map();
        for (String metric : knnStats.getClusterStats().keySet()) {
            assertTrue("Cluster metric is not in response: " + metric, responseMap.containsKey(metric));
        }

        // Check node level metrics
        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap = (Map<String, Object>)responseMap.get("nodes");

        // The key associated with the node that made the request
        String key = (String)nodesResponseMap.keySet().toArray()[0];

        @SuppressWarnings("unchecked")
        Map<String, Object> metricMap = (Map<String, Object>) nodesResponseMap.get(key);

        // Confirm that all node level metrics are returned
        Map<String, KNNStat<?>> nodeStats = knnStats.getNodeStats();
        assertEquals("Incorrect number of metrics returned", nodeStats.size(), metricMap.size());
        for (String metric : nodeStats.keySet()) {
            assertTrue("Metric should not be in response: " + metric, metricMap.containsKey(metric));
        }
    }

    /**
     *  Test checks that handler correctly returns value for select metrics
     * @throws IOException throws IOException
     */
    public void testStatsValueCheck() throws IOException {
        // Setup request for stat calls
        Request statsRequest = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/stats"
        );

        // Get initial stats as baseline
        String statsResponseBody = makeRequestAndReturnResponseBody(statsRequest);
        Map<String, Object> responseMap0 = createParser(XContentType.JSON.xContent(), statsResponseBody).map();
        assertNotNull("Stats response 0 is null", responseMap0);

        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap0 = (Map<String, Object>)responseMap0.get("nodes");
        assertNotNull("Stats node response 0 is null", nodesResponseMap0);

        Object[] keys = nodesResponseMap0.keySet().toArray();
        assertTrue("No node keys returned", keys.length > 0);
        String key = (String) keys[0];

        @SuppressWarnings("unchecked")
        Map<String, Object> metricMap0 = (Map<String, Object>) nodesResponseMap0.get(key);
        Integer initialHitCount = (Integer) metricMap0.get(StatNames.HIT_COUNT.getName());
        Integer initialMissCount = (Integer) metricMap0.get(StatNames.MISS_COUNT.getName());

        // Setup index
        Settings settings = Settings.builder()
                                    .put("number_of_shards", 1)
                                    .put("number_of_replicas", 0)
                                    .put("index.knn", true)
                                    .build();
        String index = "testindex";
        createIndex(index, settings);

        // Put KNN mapping
        Request mappingRequest = new Request(
                "PUT",
                "/" + index + "/_mapping"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                                                 .startObject("properties")
                                                 .startObject("my_vector")
                                                 .field("type", "knn_vector")
                                                 .field("dimension", "2")
                                                 .endObject()
                                                 .endObject()
                                                 .endObject();

        mappingRequest.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(mappingRequest);
        assertEquals(mappingRequest.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Index test document
        Request indexRequest = new Request(
                "POST",
                "/" + index + "/_doc/1?refresh=true" // refresh=true ensures document is searchable immediately after index
        );

        float[] vector = {6.0f, 6.0f};

        builder = XContentFactory.jsonBuilder().startObject()
                                 .field("my_vector", vector)
                                 .endObject();

        indexRequest.setJsonEntity(Strings.toString(builder));

        response = client().performRequest(indexRequest);
        assertEquals(indexRequest.getEndpoint() + ": failed", RestStatus.CREATED, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // First search: Ensure that misses=1
        response = makeGenericKnnQuery(index, vector, 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        statsResponseBody = makeRequestAndReturnResponseBody(statsRequest);
        Map<String, Object> responseMap1 = createParser(XContentType.JSON.xContent(), statsResponseBody).map();
        assertNotNull("Stats response 1 is null", responseMap1);

        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap1 = (Map<String, Object>)responseMap1.get("nodes");
        assertNotNull("Stats node response 1 is null", nodesResponseMap1);

        @SuppressWarnings("unchecked")
        Map<String, Object> metricMap1 = (Map<String, Object>) nodesResponseMap1.get(key);
        assertNotNull("Stats metric map response 1 is null", metricMap1);
        assertTrue("Miss and hit count does not return expected",
                (Integer) metricMap1.get(StatNames.MISS_COUNT.getName()) == initialMissCount + 1 &&
                        metricMap1.get(StatNames.HIT_COUNT.getName()) == initialHitCount);

        // Second search: Ensure that hits=1
        response = makeGenericKnnQuery(index, vector, 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        statsResponseBody = makeRequestAndReturnResponseBody(statsRequest);
        Map<String, Object> responseMap2 = createParser(XContentType.JSON.xContent(), statsResponseBody).map();
        assertNotNull("Stats response 2 is null", responseMap2);

        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap2 = (Map<String, Object>)responseMap2.get("nodes");
        assertNotNull("Stats node response 2 is null", nodesResponseMap2);

        @SuppressWarnings("unchecked")
        Map<String, Object> metricMap2 = (Map<String, Object>) nodesResponseMap2.get(key);
        assertNotNull("Stats metric map response 2 is null", metricMap2);
        assertTrue("Miss and hit count does not return expected",
                (Integer) metricMap2.get(StatNames.HIT_COUNT.getName()) == initialHitCount + 1 &&
                        (Integer) metricMap2.get(StatNames.MISS_COUNT.getName()) == initialMissCount + 1);
    }

    /**
     *  Test checks that handler correctly returns selected metrics
     * @throws IOException throws IOException
     */
    public void testValidMetricsStats() throws IOException {
        // Create request that only grabs two of the possible metrics
        String metric1 = StatNames.HIT_COUNT.getName();
        String metric2 = StatNames.MISS_COUNT.getName();
        Request request = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/stats/" + metric1 + "," + metric2
        );

        Response response = client().performRequest(request);

        // Check that the call succeeded
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Check that metric 1 and 2 are the only metrics in the response
        String responseBody = EntityUtils.toString(response.getEntity());;

        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap = (Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody).map().get("nodes");

        String key = (String)nodesResponseMap.keySet().toArray()[0];

        @SuppressWarnings("unchecked")
        Set<String> metricSet = ((Map<String, Object>) nodesResponseMap.get(key)).keySet();

        assertEquals("Incorrect number of metrics returned", 2, metricSet.size());
        assertTrue("does not contain correct metric: " + metric1, metricSet.contains(metric1));
        assertTrue("does not contain correct metrics: " + metric2, metricSet.contains(metric2));
    }

    /**
     *  Test checks that handler correctly returns failure on an invalid metric
     * @throws Exception throws exception
     */
    public void testInvalidMetricsStats() throws Exception {
        Request request = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/stats/invalid_metric"
        );

        assertFailWith(ResponseException.class, null, () -> client().performRequest(request));
    }

    /**
     *  Test checks that handler correctly returns stats for a single node
     * @throws IOException throws IOException
     */
    public void testValidNodeIdStats() throws IOException {
        Request request = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/_local/stats"
        );

        Response response = client().performRequest(request);

        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    /**
     *  Test checks that handler correctly returns failure on an invalid node
     * @throws Exception throws Exception
     */
    public void testInvalidNodeIdStats() throws Exception {
        Request request = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + "/invalid_nodeid/stats"
        );

        Response response = client().performRequest(request);

        // Check that the call succeeded, but had no nodes return values
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());;

        @SuppressWarnings("unchecked")
        Map<String, Object> nodesResponseMap = (Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody).map().get("nodes");

        assertEquals("Incorrect number of metrics returned", 0, nodesResponseMap.keySet().size());
    }

    /**
     * Assertion checks to see if callable fails as expected
     * @param clazz Exception class expected
     * @param message Message thrown on failure to correctly fail
     * @param callable Lambda to call
     * @param <S> Class template
     * @param <T> Callable template
     * @throws Exception throws exception
     */
    private static <S,T> void assertFailWith(Class<S> clazz, String message, Callable<T> callable) throws Exception {
        try {
            callable.call();
        } catch (Throwable e) {
            if (e.getClass() != clazz) {
                throw e;
            }
            if (message != null && !e.getMessage().contains(message)) {
                throw e;
            }
        }
    }

    /**
     * Helper method to make a request, assert that it is valid, and return the response body
     * @param request request to be executed
     * @return response body
     * @throws IOException throws IO exception
     */
    private String makeRequestAndReturnResponseBody(Request request) throws IOException {
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return EntityUtils.toString(response.getEntity());
    }

    /**
     * Helper method to generate a generic knn query for testing purposes
     * @param index index name
     * @param vector vector to be searched for
     * @param k k nearest neighbors
     * @throws IOException throws IO exception
     */
    private Response makeGenericKnnQuery(String index, float[] vector, int k) throws IOException {
        Request request = new Request(
                "POST",
                "/" + index + "/_search"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                                                 .startObject("query")
                                                 .startObject("knn")
                                                 .startObject("my_vector")
                                                 .field("vector", vector)
                                                 .field("k",k)
                                                 .endObject()
                                                 .endObject()
                                                 .endObject()
                                                 .endObject();

        request.setJsonEntity(Strings.toString(builder));
        return client().performRequest(request);
    }

    // Useful settings when debugging to prevent timeouts
    @Override
    protected Settings restClientSettings() {
        if (isDebuggingTest || isDebuggingRemoteCluster) {
            return Settings.builder()
                           .put(CLIENT_SOCKET_TIMEOUT, TimeValue.timeValueMinutes(10))
                           .build();
        } else {
            return super.restClientSettings();
        }
    }
}
