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

import org.elasticsearch.test.ESTestCase;

public class KNNCounterTests extends ESTestCase {
    public void testGetName() {
        assertEquals(StatNames.GRAPH_QUERY_ERRORS.getName(), KNNCounter.GRAPH_QUERY_ERRORS.getName());
    }

    public void testCount() {
        assertEquals((Long) 0L, KNNCounter.GRAPH_QUERY_ERRORS.getCount());

        for (long i = 0; i < 100; i++) {
            KNNCounter.GRAPH_QUERY_ERRORS.increment();
            assertEquals((Long) (i+1), KNNCounter.GRAPH_QUERY_ERRORS.getCount());
        }
    }
}