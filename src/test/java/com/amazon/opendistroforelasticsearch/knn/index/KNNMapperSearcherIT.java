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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNMapperSearcherIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KNNMapperSearcherIT.class);

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
     * Test Data set
     */
    private void addTestData() throws Exception {

        Float[] f1  = {6.0f, 6.0f};
        addKnnDoc("testindex", "1", f1);

        Float[] f2  = {2.0f, 2.0f};
        addKnnDoc("testindex", "2", f2);

        Float[] f3  = {4.0f, 4.0f};
        addKnnDoc("testindex", "3", f3);

        Float[] f4  = {3.0f, 3.0f};
        addKnnDoc("testindex", "4", f4);
    }

    private void addKnnDoc(String index, String docId, Object[] vector) throws IOException {
        IndexResponse response = client().prepareIndex(index, "_doc", docId)
                                         .setSource(XContentFactory.jsonBuilder()
                                                                   .startObject()
                                                                   .array("my_vector", vector)
                                                                   .field("price", 10)
                                                                   .endObject())
                                         .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                         .get();
        if(!response.status().equals(RestStatus.OK) && !response.status().equals(RestStatus.CREATED)) {
            fail("Bad response while adding doc");
        }
    }


    private void createKnnIndex(String index) {
        createIndex(index, indexSettings());
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");
        request.source("my_vector", "type=knn_vector,dimension=2");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());
    }

    private SearchResponse searchKNNIndex(String index, int resultSize, KNNQueryBuilder knnQueryBuilder) {
        logger.info("Searching KNN index " + index );
        SearchResponse searchResponse = client().prepareSearch(index)
                                                 .setSearchType(SearchType.QUERY_THEN_FETCH)
                                                 .setQuery(knnQueryBuilder)  // Query
                                                 .setSize(resultSize)
                                                 .setExplain(true)
                                                 .get();
        assertEquals(searchResponse.status(), RestStatus.OK);
        return searchResponse;
    }

    private void forceMergeKnnIndex(String index) throws Exception {
        client().admin().indices().refresh(new RefreshRequest(index)).actionGet();
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index);
        forceMergeRequest.maxNumSegments(1);
        forceMergeRequest.flush(true);
        ForceMergeResponse forceMergeResponse =client().admin().indices().forceMerge(forceMergeRequest).actionGet();
        assertEquals(forceMergeResponse.getStatus(), RestStatus.OK);
        TimeUnit.SECONDS.sleep(5); // To make sure force merge is completed
    }

    private SearchResponse doQueryKnn(float[] queryVector, int k) throws Exception {
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("my_vector", queryVector, k);
        forceMergeKnnIndex("testindex");
        return searchKNNIndex("testindex", k, knnQueryBuilder);
    }


    public void testKNNResultsWithForceMerge() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        /**
         * Query params
         */
        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("my_vector", queryVector, k);

        forceMergeKnnIndex("testindex");

        SearchResponse searchResponse;
        searchResponse = searchKNNIndex("testindex", 10, knnQueryBuilder);

        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "2"); //Vector of DocId 2 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);
    }

    public void testKNNResultsWithoutForceMerge() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        /**
         * Query params
         */
        float[] queryVector = {2.0f, 2.0f}; // vector to be queried
        int k = 3; //nearest 3 neighbors
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("my_vector", queryVector, k);

        SearchResponse searchResponse;

        searchResponse = searchKNNIndex("testindex", k, knnQueryBuilder);
        List<String> expectedDocids = Arrays.asList("2", "4", "3");

        List<String> actualDocids = new ArrayList<>();
        for(SearchHit hit : searchResponse.getHits()) {
            actualDocids.add(hit.getId());
        }

        assertEquals(actualDocids.size(), k);
        assertArrayEquals(actualDocids.toArray(), expectedDocids.toArray());
    }

    public void testKNNResultsWithNewDoc() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        SearchResponse searchResponse;
        searchResponse = doQueryKnn(queryVector, k);
        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "2"); //Vector of DocId 2 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);

        /**
         * Add new doc with vector not nearest than doc 2
         */
        Float[] newVector  = {6.0f, 6.0f};
        addKnnDoc("testindex", "6", newVector);
        searchResponse = doQueryKnn(queryVector, k);

        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "2"); //Vector of DocId 2 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);

        /**
         * Add new doc with vector nearest than doc 2 to queryVector
         */
        Float[] newVector1  = {0.5f, 0.5f};
        addKnnDoc("testindex", "7", newVector1);
        searchResponse = doQueryKnn(queryVector, k);

        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "7"); //Vector of DocId 7 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);
    }

    public void testKNNResultsWithUpdateDoc() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        SearchResponse searchResponse;
        searchResponse = doQueryKnn(queryVector, k);
        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "2"); //Vector of DocId 2 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);

        /**
         * update doc 3 to the nearest
         */
        Float[] updatedVector  = {0.1f, 0.1f};
        addKnnDoc("testindex", "3", updatedVector);
        searchResponse = doQueryKnn(queryVector, k);
        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "3"); //Vector of DocId 3 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);
    }

    public void testKNNResultsWithDeleteDoc() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor
        SearchResponse searchResponse;
        searchResponse = doQueryKnn(queryVector, k);
        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "2"); //Vector of DocId 2 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);

        /**
         * delete the nearest doc (doc2)
         */
        DeleteResponse response = client().prepareDelete("testindex", "_doc", "2")
                                          .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                          .get();
        assertEquals(RestStatus.OK, response.status());

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("my_vector", queryVector, k+1);
        searchResponse = searchKNNIndex("testindex", k, knnQueryBuilder);
        for(SearchHit hit : searchResponse.getHits()) {
            assertEquals(hit.getId(), "4"); //Vector of DocId 4 is closest to the query
        }
        ElasticsearchAssertions.assertHitCount(searchResponse, k);
    }

    /**
     * For negative K, query builder should throw Exception
     */
    public void testNegativeK() {
        float[] vector = {1.0f, 2.0f};
        expectThrows(IllegalArgumentException.class, () -> new KNNQueryBuilder("myvector", vector, -1));
    }

    /**
     *  For zero K, query builder should throw Exception
     */
    public void testZeroK() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 0; //  nearest 1 neighbor
        expectThrows(IllegalArgumentException.class, () -> doQueryKnn(queryVector, k));
    }

    /**
     * K &gt; &gt; number of docs
     */
    public void testLargeK() throws Exception {
        createKnnIndex("testindex");
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = KNNQueryBuilder.K_MAX; //  nearest 1 neighbor

        SearchResponse searchResponse;
        searchResponse = doQueryKnn(queryVector, k);
        ElasticsearchAssertions.assertHitCount(searchResponse, 4);
    }
}
