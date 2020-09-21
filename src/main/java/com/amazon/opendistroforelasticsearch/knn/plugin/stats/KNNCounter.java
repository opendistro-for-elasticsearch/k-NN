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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Contains a map of counters to keep track of different values
 */
public enum KNNCounter {
    GRAPH_QUERY_ERRORS("graph_query_errors"),
    GRAPH_QUERY_REQUESTS("graph_query_requests"),
    GRAPH_INDEX_ERRORS("graph_index_errors"),
    GRAPH_INDEX_REQUESTS("graph_index_requests"),
    KNN_QUERY_REQUESTS("knn_query_requests"),
    SCRIPT_COMPILATIONS("script_compilations"),
    SCRIPT_COMPILATION_ERRORS("script_compilation_errors"),
    SCRIPT_QUERY_REQUESTS("script_query_requests"),
    SCRIPT_QUERY_ERRORS("script_query_errors");

    private String name;
    private AtomicLong count;

    /**
     * Constructor
     *
     * @param name name of the counter
     */
    KNNCounter(String name) {
        this.name = name;
        this.count = new AtomicLong(0);
    }

    /**
     * Get name of counter
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the value of count
     *
     * @return count
     */
    public Long getCount() {
        return count.get();
    }

    /**
     * Increment the value of a counter
     */
    public void increment() {
        count.getAndIncrement();
    }

    /**
     * @param value counter value
     * Set the value of a counter
     */
    public void set(long value) {
        count.set(value);
    }
}