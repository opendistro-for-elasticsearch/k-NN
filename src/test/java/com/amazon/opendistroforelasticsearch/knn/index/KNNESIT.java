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

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;
import com.amazon.opendistroforelasticsearch.knn.KNNResult;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class KNNESIT extends KNNRestTestCase {
    /**
     * Able to add docs to KNN index
     */
    public void testAddKNNDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);
    }

    /**
     * Block adding new docs to KNN index when circuit breaker trips
     */
    public void testAddKNNDocBlockedWhenCbTrips() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        updateClusterSettings("knn.circuit_breaker.triggered", "true");

        Float[] vector  = {6.0f, 6.0f};
        ResponseException ex = expectThrows(
                ResponseException.class, () -> addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector));
        String expMessage = "Indexing knn vector fields is rejected as circuit breaker triggered." +
                " Check _opendistro/_knn/stats for detailed state";
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString(expMessage));

        // reset
        updateClusterSettings("knn.circuit_breaker.triggered", "false");
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);
    }

    /**
     * Able to update docs in KNN index
     */
    public void testUpdateKNNDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        // update
        Float[] updatedVector  = {8.0f, 8.0f};
        updateKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);
    }

    /**
     * Block updating docs under KNN index when circuit breaker trips
     */
    public void testUpdateKNNDocBlockedWhenCbTrips() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        // update
        updateClusterSettings("knn.circuit_breaker.triggered", "true");
        Float[] updatedVector  = {8.0f, 8.0f};
        ResponseException ex = expectThrows(
                ResponseException.class, () -> updateKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector));
        String expMessage = "Indexing knn vector fields is rejected as circuit breaker triggered." +
                " Check _opendistro/_knn/stats for detailed state";
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString(expMessage));

        // reset
        updateClusterSettings("knn.circuit_breaker.triggered", "false");
        updateKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);
    }

    /**
     * Able to delete docs in KNN index
     */
    public void testDeleteKNNDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        // delete knn doc
        deleteKnnDoc(INDEX_NAME, "1");
    }

    /**
     * Create knn index with valid index algo params
     */
    public void testCreateIndexWithValidAlgoParams() {
        try {
            Settings settings = Settings.builder()
                                        .put(getKNNDefaultIndexSettings())
                                        .put("index.knn.algo_param.m", 32)
                                        .put("index.knn.algo_param.ef_construction", 400)
                                        .build();
            createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
            Float[] vector = {6.0f, 6.0f};
            addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);
        } catch (Exception ex) {
            fail("Exception not expected as valid index arguements passed: " + ex);
        }
    }

    /**
     * Create knn index with valid query algo params
     */
    public void testQueryIndexWithValidQueryAlgoParams() throws IOException {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .put("index.knn.algo_param.ef_search", 300)
                                    .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
    }

    public void testAddAndSearchIndexWhenCBTrips() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        for (int i=1; i<=4; i++) {
            Float[] vector  = {(float)i, (float)(i+1)};
            addKnnDoc(INDEX_NAME, Integer.toString(i), FIELD_NAME, vector);
        }

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 10; //  nearest 10 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(4, results.size());

        updateClusterSettings("knn.circuit_breaker.triggered", "true");
        // Try add another doc
        Float[] vector  = {1.0f, 2.0f};
        ResponseException ex = expectThrows(
                ResponseException.class, () -> addKnnDoc(INDEX_NAME, "5", FIELD_NAME, vector));

        // Still get 4 docs
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(4, results.size());

        updateClusterSettings("knn.circuit_breaker.triggered", "false");
        addKnnDoc(INDEX_NAME, "5", FIELD_NAME, vector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(5, results.size());
    }

    public void testIndexingVectorValidationDifferentSizes() throws Exception {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .build();

        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 4));

        /**
         * valid case with 4 dimension
         */
        Float[] vector = {6.0f, 7.0f, 8.0f, 9.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        /**
         * invalid case with lesser dimension than original (3 < 4)
         */
        Float[] vector1 = {6.0f, 7.0f, 8.0f};
        ResponseException ex = expectThrows(ResponseException.class, () ->
                addKnnDoc(INDEX_NAME, "2", FIELD_NAME, vector1));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Vector dimension mismatch. Expected: 4, Given: 3"));

        /**
         * invalid case with more dimension than original (5 > 4)
         */
        Float[] vector2 = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
        ex = expectThrows(ResponseException.class, () -> addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector2));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Vector dimension mismatch. Expected: 4, Given: 5"));
    }

    public void testVectorMappingValidationNoDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .build();

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject(FIELD_NAME)
                .field("type", "knn_vector")
                .endObject()
                .endObject()
                .endObject());

        Exception ex = expectThrows(ResponseException.class, () -> createKnnIndex(INDEX_NAME, settings, mapping));
        assertThat(ex.getMessage(), containsString("Dimension value missing for vector: " + FIELD_NAME));
    }

    public void testVectorMappingValidationInvalidDimension() {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .build();

        Exception ex = expectThrows(ResponseException.class, () -> createKnnIndex(INDEX_NAME, settings,
                createKnnIndexMapping(FIELD_NAME, KNNVectorFieldMapper.MAX_DIMENSION + 1)));
        assertThat(ex.getMessage(), containsString("Dimension value cannot be greater than " +
                KNNVectorFieldMapper.MAX_DIMENSION + " for vector: " + FIELD_NAME));
    }

    public void testVectorMappingValidationInvalidVectorNaN() throws IOException {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .build();

        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));

        Float[] vector = {Float.NaN, Float.NaN};
        Exception ex = expectThrows(ResponseException.class, () -> addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector));
        assertThat(ex.getMessage(), containsString("KNN vector values cannot be NaN"));
    }

    public void testVectorMappingValidationInvalidVectorInfinity() throws IOException {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .build();

        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));

        Float[] vector = {Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY};
        Exception ex = expectThrows(ResponseException.class, () -> addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector));
        assertThat(ex.getMessage(), containsString("KNN vector values cannot be infinity"));
    }

    public void testVectorMappingValidationUpdateDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .build();

        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 4));

        Exception ex = expectThrows(ResponseException.class, () ->
                putMappingRequest(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 5)));
        assertThat(ex.getMessage(), containsString("Dimension value cannot be updated. Previous value: 4, Current value: 5"));
    }

    /**
     * multiple fields different dimensions
     */
    public void testVectorMappingValidationMultiFieldsDifferentDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .build();

        String f4 = FIELD_NAME + "-4";
        String f5 = FIELD_NAME + "-5";
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject(f4)
                .field("type", "knn_vector")
                .field("dimension", "4")
                .endObject()
                .startObject(f5)
                .field("type", "knn_vector")
                .field("dimension", "5")
                .endObject()
                .endObject()
                .endObject());

        createKnnIndex(INDEX_NAME, settings, mapping);

        /**
         * valid case with 4 dimension
         */
        Float[] vector = {6.0f, 7.0f, 8.0f, 9.0f};
        addKnnDoc(INDEX_NAME, "1", f4, vector);

        /**
         * valid case with 5 dimension
         */
        Float[] vector1 = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
        updateKnnDoc(INDEX_NAME, "1", f5, vector1);
    }

    public void testInvalidIndexHnswAlgoParams() {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put("index.knn.algo_param.m", "-1")
                .build();
        Exception ex = expectThrows(ResponseException.class, () -> createKnnIndex(INDEX_NAME, settings,
                createKnnIndexMapping(FIELD_NAME, 2)));
        assertThat(ex.getMessage(), containsString("Failed to parse value [-1] for setting [index.knn.algo_param.m]"));
    }

    public void testInvalidQueryHnswAlgoParams() {
        Settings settings = Settings.builder()
                                    .put(getKNNDefaultIndexSettings())
                                    .put("index.knn.algo_param.ef_search", "-1")
                                    .build();
        Exception ex = expectThrows(ResponseException.class, () -> createKnnIndex(INDEX_NAME, settings,
                createKnnIndexMapping(FIELD_NAME, 2)));
        assertThat(ex.getMessage(), containsString("Failed to parse value [-1] for setting [index.knn.algo_param.ef_search]"));
    }
}
