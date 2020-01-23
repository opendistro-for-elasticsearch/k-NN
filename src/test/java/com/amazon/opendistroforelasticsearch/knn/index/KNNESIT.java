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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNESIT extends BaseKNNIntegTestIT {

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                       .put(super.indexSettings())
                       .put("number_of_shards", 1)
                       .put("number_of_replicas", 0)
                       .put("index.knn", true)
                       .build();
    }

    /**
     * Able to add docs to KNN index
     */
    public void testAddKNNDoc() throws Exception {
        createKnnIndex("testindex", null);
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);
    }

    /**
     * Able to update docs in KNN index
     */
    public void testUpdateKNNDoc() throws Exception {
        createKnnIndex("testindex", null);
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);

        // update
        Float[] updatedVector  = {8.0f, 8.0f};
        addKnnDoc("testindex", "1", vector);
    }

    /**
     * Able to delete docs in KNN index
     */
    public void testDeleteKNNDoc() throws Exception {
        createKnnIndex("testindex", null);
        Float[] vector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", vector);

        // delete knn doc
        DeleteResponse response = client().prepareDelete("testindex", "_doc", "1")
                                          .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                          .get();
        assertEquals(RestStatus.OK, response.status());
    }

    /**
     * Create knn index with valid index algo params
     */
    public void testCreateIndexWithValidAlgoParams() throws Exception {
        try {
            Settings settings = Settings.builder()
                                        .put(super.indexSettings())
                                        .put("index.knn", true)
                                        .put("index.knn.algo_param.m", 32)
                                        .put("index.knn.algo_param.ef_construction", 400)
                                        .build();
            createKnnIndex("testindex", settings);
            Float[] vector = {6.0f, 6.0f};
            addKnnDoc("testindex", "1", vector);
        } catch (Exception ex) {
            fail("Exception not expected as valid index arguements passed");
        }
    }

    /**
     * Create knn index with valid query algo params
     */
    public void testQueryIndexWithValidQueryAlgoParams() throws Exception {
        try {
            Settings settings = Settings.builder()
                                        .put(super.indexSettings())
                                        .put("index.knn", true)
                                        .put("index.knn.algo_param.ef_search", 300)
                                        .build();
            createKnnIndex("testindex", settings);
            Float[] vector = {6.0f, 6.0f};
            addKnnDoc("testindex", "1", vector);

            float[] queryVector = {1.0f, 1.0f}; // vector to be queried
            int k = 1; //  nearest 1 neighbor
            KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("my_vector", queryVector, k);
            searchKNNIndex("testindex", k, knnQueryBuilder);
        } catch (Exception ex) {
            fail("Exception not expected as valid index arguements passed");
        }
    }

    public void testIndexingVectorValidationDifferentSizes() throws Exception {
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .build();

        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source("my_vector", "type=knn_vector,dimension=4");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());

        /**
         * valid case with 4 dimension
         */
        Float[] vector = {6.0f, 7.0f, 8.0f, 9.0f};
        addKnnDoc(index, "1", vector);

        /**
         * invalid case with lesser dimension than original (3 < 4)
         */
        Float[] vector1 = {6.0f, 7.0f, 8.0f};
        Exception ex = expectThrows(MapperParsingException.class, () -> addKnnDoc(index, "2", vector1));
        assertThat(ex.getCause().getMessage(), containsString("Vector dimension mismatch. Expected: 4, Given: 3"));

        /**
         * invalid case with more dimension than original (5 > 4)
         */
        Float[] vector2 = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
        ex = expectThrows(MapperParsingException.class, () -> addKnnDoc(index, "3", vector2));
        assertThat(ex.getCause().getMessage(), containsString("Vector dimension mismatch. Expected: 4, Given: 5"));
    }

    public void testVectorMappingValidationNoDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .build();

        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source("my_vector", "type=knn_vector");
        Exception ex = expectThrows(MapperParsingException.class, () -> ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet()));
        assertThat(ex.getMessage(), containsString("Dimension value missing for vector: my_vector"));
    }

    public void testVectorMappingValidationInvalidDimension() throws Exception {
        Settings settings = Settings.builder()
                .put(super.indexSettings())
                .put("index.knn", true)
                .build();

        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source("my_vector", "type=knn_vector,dimension=" + (KNNVectorFieldMapper.MAX_DIMENSION + 1));
        Exception ex = expectThrows(MapperParsingException.class, () -> ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet()));
        assertThat(ex.getMessage(), containsString("Dimension value cannot be greater than " +
                KNNVectorFieldMapper.MAX_DIMENSION + " for vector: my_vector"));
    }

    public void testVectorMappingValidationUpdateDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .build();

        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source("my_vector", "type=knn_vector,dimension=4");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());


        request.source("my_vector", "type=knn_vector,dimension=5");
        Exception ex = expectThrows(MapperParsingException.class, () -> ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet()));
        assertThat(ex.getMessage(), containsString("Dimension value cannot be updated. Previous value: 4, Current value: 5"));
    }

    /**
     * multiple fields different dimensions
     */
    public void testVectorMappingValidationMultiFieldsDifferentDimension() throws Exception {
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .build();

        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");

        request.source("my_vector", "type=knn_vector,dimension=4");
        request.source("my_vector1", "type=knn_vector,dimension=5");

        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());

        /**
         * valid case with 4 dimension
         */
        Float[] vector = {6.0f, 7.0f, 8.0f, 9.0f};
        addKnnDoc(index, "1", vector);

        /**
         * valid case with 5 dimension
         */
        Float[] vector1 = {6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
        addKnnDocWithField(index, "1", vector1, "my_vector1");
    }

    public void testInvalidIndexHnswAlgoParams() throws Exception {
        String index = "testindex";
        Settings settings = Settings.builder()
                .put(super.indexSettings())
                .put("index.knn", true)
                .put("index.knn.algo_param.m", "-1")
                .build();
        Exception ex = expectThrows(IllegalArgumentException.class, () -> createIndex(index, settings));
        assertThat(ex.getMessage(), containsString("Failed to parse value [-1] for setting [index.knn.algo_param.m]"));
    }

    public void testInvalidQueryHnswAlgoParams() throws Exception {
        String index = "testindex";
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .put("index.knn.algo_param.ef_search", "-1")
                                    .build();
        Exception ex = expectThrows(IllegalArgumentException.class, () -> createIndex(index, settings));
        assertThat(ex.getMessage(), containsString("Failed to parse value [-1] for setting [index.knn.algo_param.ef_search]"));
    }
}
