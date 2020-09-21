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

package com.amazon.opendistroforelasticsearch.knn.plugin.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum contains names of the stats
 */
public enum StatNames {
    HIT_COUNT("hit_count"),
    MISS_COUNT("miss_count"),
    LOAD_SUCCESS_COUNT("load_success_count"),
    LOAD_EXCEPTION_COUNT("load_exception_count"),
    TOTAL_LOAD_TIME("total_load_time"),
    EVICTION_COUNT("eviction_count"),
    GRAPH_MEMORY_USAGE("graph_memory_usage"),
    GRAPH_MEMORY_USAGE_PERCENTAGE("graph_memory_usage_percentage"),
    CACHE_CAPACITY_REACHED("cache_capacity_reached"),
    INDICES_IN_CACHE("indices_in_cache"),
    CIRCUIT_BREAKER_TRIGGERED("circuit_breaker_triggered"),
    GRAPH_QUERY_ERRORS(KNNCounter.GRAPH_QUERY_ERRORS.getName()),
    GRAPH_QUERY_REQUESTS(KNNCounter.GRAPH_QUERY_REQUESTS.getName()),
    GRAPH_INDEX_ERRORS(KNNCounter.GRAPH_INDEX_ERRORS.getName()),
    GRAPH_INDEX_REQUESTS(KNNCounter.GRAPH_INDEX_REQUESTS.getName()),
    KNN_QUERY_REQUESTS(KNNCounter.KNN_QUERY_REQUESTS.getName()),
    SCRIPT_COMPILATIONS(KNNCounter.SCRIPT_COMPILATIONS.getName()),
    SCRIPT_COMPILATION_ERRORS(KNNCounter.SCRIPT_COMPILATION_ERRORS.getName()),
    SCRIPT_QUERY_REQUESTS(KNNCounter.SCRIPT_QUERY_REQUESTS.getName()),
    SCRIPT_QUERY_ERRORS(KNNCounter.SCRIPT_QUERY_ERRORS.getName());

    private String name;

    StatNames(String name) { this.name = name; }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() { return name; }

    /**
     * Get all stat names
     *
     * @return set of all stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}