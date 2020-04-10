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

package com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers;

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.elasticsearch.test.ESTestCase;

public class KNNCounterSupplierTests extends ESTestCase {
    public void testNormal() {
        KNNCounterSupplier knnCounterSupplier = new KNNCounterSupplier(KNNCounter.GRAPH_QUERY_REQUESTS);
        assertEquals((Long) 0L, knnCounterSupplier.get());
        KNNCounter.GRAPH_QUERY_REQUESTS.increment();
        assertEquals((Long) 1L, knnCounterSupplier.get());
    }
}