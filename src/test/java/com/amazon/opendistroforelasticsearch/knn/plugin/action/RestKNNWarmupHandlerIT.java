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

package com.amazon.opendistroforelasticsearch.knn.plugin.action;

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Integration tests to check the correctness of KNN Warmup API
 */

public class RestKNNWarmupHandlerIT extends KNNRestTestCase {

    private final String testIndexName = "test-index";
    private final String testFieldName = "test-field";
    private final int dimensions = 2;

    @Test(expected = ResponseException.class)
    public void testNonExistentIndex() throws IOException {
        knnWarmup(Collections.singletonList("non-existent"));
    }

    @Test(expected = ResponseException.class)
    public void testNonKnnIndex() throws IOException {
        createIndex("not-knn-index", Settings.EMPTY);

        knnWarmup(Collections.singletonList("not-knn-index"));
    }

    public void testEmptyIndex() throws IOException {
        int graphCountBefore = getTotalGraphsInCache();
        createKnnIndex(testIndexName, getKNNDefaultIndexSettings(), createKnnIndexMapping(testFieldName, dimensions));

        knnWarmup(Collections.singletonList(testIndexName));

        assertEquals(graphCountBefore, getTotalGraphsInCache());
    }

    public void testSingleIndex() throws IOException {
        int graphCountBefore = getTotalGraphsInCache();
        createKnnIndex(testIndexName, getKNNDefaultIndexSettings(), createKnnIndexMapping(testFieldName, dimensions));
        addKnnDoc(testIndexName, "1", testFieldName, new Float[] {6.0f, 6.0f});

        knnWarmup(Collections.singletonList(testIndexName));

        assertEquals(graphCountBefore + 1, getTotalGraphsInCache());
    }

    public void testMultipleIndices() throws IOException {
        int graphCountBefore = getTotalGraphsInCache();

        createKnnIndex(testIndexName + "1", getKNNDefaultIndexSettings(), createKnnIndexMapping(testFieldName, dimensions));
        addKnnDoc(testIndexName + "1", "1", testFieldName, new Float[] {6.0f, 6.0f});

        createKnnIndex(testIndexName + "2", getKNNDefaultIndexSettings(), createKnnIndexMapping(testFieldName, dimensions));
        addKnnDoc(testIndexName + "2", "1", testFieldName, new Float[] {6.0f, 6.0f});

        knnWarmup(Arrays.asList(testIndexName + "1", testIndexName + "2"));

        assertEquals(graphCountBefore + 2, getTotalGraphsInCache());
    }
}
