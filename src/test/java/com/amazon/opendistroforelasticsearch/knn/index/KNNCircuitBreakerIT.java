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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNCircuitBreaker.CB_TIME_INTERVAL;

/**
 * Integration tests to test Circuit Breaker functionality
 */
public class KNNCircuitBreakerIT extends BaseKNNIntegTestIT {
    /**
     * Utility function that sets the cb limit low enough that ingesting a couple hundred documents trips it
     */
    private void tripCb() throws Exception {
        // Make sure that Cb is intially not tripped
        assertFalse(isCbTripped());

        // Set circuit breaker limit to 1 KB
        updateClusterSettings("knn.memory.circuit_breaker.limit", "1kb");

        // Create Single Shard Index so that all data is hosted on a single node
        Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn", true)
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));

        // Index 20 dummy documents
        Float[] vector = {1.3f, 2.2f};
        for (int i = 0; i < 10; i++) {
            addKnnDoc(INDEX_NAME, Integer.toString(i), FIELD_NAME, vector);
        }

        // Execute search
        float[] qvector = {1.9f, 2.4f};
        int k = 10;
        searchKNNIndex(INDEX_NAME, new KNNQueryBuilder(FIELD_NAME, qvector, k), k);

        // Assert that Cb get triggered
        assertTrue(isCbTripped());
    }

    public boolean isCbTripped() throws Exception {
        Response response = getKnnStats(Collections.emptyList(),
                Collections.singletonList("circuit_breaker_triggered"));
        String responseBody = EntityUtils.toString(response.getEntity());
        Map<String, Object> clusterStats = parseClusterStatsResponse(responseBody);
        return (Boolean) clusterStats.get("circuit_breaker_triggered");
    }

    public void testCbTripped() throws Exception {
        tripCb();
    }

    public void testCbUntrips() throws Exception {
        updateClusterSettings("knn.circuit_breaker.triggered", "true");
        //TODO: Attempt to find a better way to trigger runnable than waiting 2 minutes for it to finish
        Thread.sleep(CB_TIME_INTERVAL*1000);
        assertFalse(isCbTripped());
    }
}