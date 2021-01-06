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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KNNMapperSearcherIT extends KNNRestTestCase {
    private static final Logger logger = LogManager.getLogger(KNNMapperSearcherIT.class);

    /**
     * Test Data set
     */
    private void addTestData() throws Exception {
        Float[] f1  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f1);

        Float[] f2  = {2.0f, 2.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, f2);

        Float[] f3  = {4.0f, 4.0f};
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, f3);

        Float[] f4  = {3.0f, 3.0f};
        addKnnDoc(INDEX_NAME, "4", FIELD_NAME, f4);
    }

    private void addBitTestData() throws  Exception {
        Float[] f1  = {0.0f, 0.0f, 0.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f1);

        Float[] f2  = {1.0f, 0.0f, 5.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, f2);

        Float[] f3  = {0.0f, 5.0f, 1.0f};
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, f3);

        Float[] f4  = {1.0f, 1.0f, 4.0f};
        addKnnDoc(INDEX_NAME, "4", FIELD_NAME, f4);
    }
    public void testKNNResultsWithForceMerge() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();
        forceMergeKnnIndex(INDEX_NAME);

        /**
         * Query params
         */
        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);

        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(k, results.size());
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId());
        }
    }

    public void testKNNResultsUpdateDocAndForceMerge() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addDocWithNumericField(INDEX_NAME, "1", "abc", 100 );
        addTestData();
        forceMergeKnnIndex(INDEX_NAME);

        /**
         * Query params
         */
        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);

        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(k, results.size());
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId());
        }
    }

    public void testKNNResultsWithoutForceMerge() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        /**
         * Query params
         */
        float[] queryVector = {2.0f, 2.0f}; // vector to be queried
        int k = 3; //nearest 3 neighbors
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);

        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "4", "3");

        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(actualDocids.size(), k);
        assertArrayEquals(actualDocids.toArray(), expectedDocids.toArray());
    }
    public void testKNNResultsWithNonOptimizedIndexAndForceMerge() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.negdotprod.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] vector1 = {-6.0f, -6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector1);
        Float[] vector2 = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, vector2);
        Float[] vector3 = {-3.0f, -3.0f};
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector3);
        forceMergeKnnIndex(INDEX_NAME);

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 2; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response searchResponse = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(searchResponse.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "3");

        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(actualDocids.size(), k);
        assertArrayEquals(actualDocids.toArray(), expectedDocids.toArray());
    }
    public void testKNNResultsWithNonOptimizedBitHammingIndexAndForceMerge() throws Exception {

        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.bit_hamming.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 96));
        Float[] vector1 = {0.0f, 0.0f, 0.0f}; //0*28, 0000, 0*28, 0000, 0*28, 0000
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector1);
        Float[] vector2 = {1.0f, 0.0f, 5.0f}; //0*28, 0001, 0*28, 0000, 0*28, 0101
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, vector2);
        Float[] vector3 = {0.0f, 5.0f, 1.0f}; //0*28, 0000, 0*28, 0101, 0*28, 0001
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector3);
        forceMergeKnnIndex(INDEX_NAME);

        //doc1: 4, doc2: 1, doc3: 3
        float[] queryVector = {1.0f, 1.0f, 5.0f}; // 0*28,0001, 0*28,0001, 0*28,0101
        int k = 3; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response searchResponse = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(searchResponse.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "3", "1");
        logger.info(results.toString());
        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(actualDocids.size(), k);
        assertArrayEquals(actualDocids.toArray(), expectedDocids.toArray());
    }

    public void testKNNResultsWithNewDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId()); //Vector of DocId 2 is closest to the query
        }

        /**
         * Add new doc with vector not nearest than doc 2
         */
        Float[] newVector  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "6", FIELD_NAME, newVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId());
        }


        /**
         * Add new doc with vector nearest than doc 2 to queryVector
         */
        Float[] newVector1  = {0.5f, 0.5f};
        addKnnDoc(INDEX_NAME, "7", FIELD_NAME, newVector1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("7", result.getDocId());
        }
    }
    public void testKNNResultsWithNonOptimizedIndexAndNewDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.negdotprod.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("1", result.getDocId()); //Vector of DocId 1 is closest to the query
        }

        /**
         * Add new doc with vector not nearest than doc 1
         */
        Float[] newVector  = {5.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "6", FIELD_NAME, newVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("1", result.getDocId());
        }


        /**
         * Add new doc with vector nearest than doc 1 to queryVector
         */
        Float[] newVector1  = {7.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "7", FIELD_NAME, newVector1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("7", result.getDocId());
        }
    }
    public void testKNNResultsWithNonOptimizedBitHammingIndexAndNewDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.bit_hamming.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 96));
/**
 *         {0,0,0}, //0*28, 0000, 0*28, 0000, 0*28, 0000 dis: = 4
 *         {1,0,5}, //0*28, 0001, 0*28, 0000, 0*28, 0101 dis: = 3
 *         {0,5,1}, //0*28, 0000, 0*28, 0101, 0*28, 0001 dis: = 1
 *         {1,1,4}, //0*28, 0001, 0*28, 0001, 0*28, 0100 dis: = 3
 */
        addBitTestData();

        float[] queryVector = {1.0f, 5.0f, 1.0f}; // //0*28, 0001, 0*28, 0101, 0*28, 0001
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 3 is closest to the query
        }

        /**
         * Add new doc with vector but not nearest than doc 3
         * 0*28, 1000, 0*28, 0100, 0*28, 0001 dis: = 3
         */
        Float[] newVector  = {8.0f, 4.0f, 1.0f};
        addKnnDoc(INDEX_NAME, "5", FIELD_NAME, newVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId());
        }


        /**
         * Add new doc with vector nearest than doc 3 to queryVector
         * 0*28, 0001, 0*28, 0101, 0*28, 0001 dis := 0
         */
        Float[] newVector1  = {1.0f, 5.0f, 1.0f}; //
        addKnnDoc(INDEX_NAME, "7", FIELD_NAME, newVector1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("7", result.getDocId());
        }
    }

    public void testKNNResultsWithUpdateDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId()); //Vector of DocId 2 is closest to the query
        }

        /**
         * update doc 3 to the nearest
         */
        Float[] updatedVector  = {0.1f, 0.1f};
        updateKnnDoc(INDEX_NAME, "3", FIELD_NAME, updatedVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 3 is closest to the query
        }
    }
    public void testKNNResultsWithNonOptimizedIndexAndUpdateDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.negdotprod.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("1", result.getDocId()); //Vector of DocId 1 is closest to the query
        }

        /**
         * update doc 3 to the nearest
         */
        Float[] updatedVector  = {6.0f, 7.0f};
        updateKnnDoc(INDEX_NAME, "3", FIELD_NAME, updatedVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 3 is closest to the query
        }
    }
    public void testKNNResultsWithNonOptimizedBitHammingIndexAndUpdateDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.bit_hamming.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 96));
        /**
         *         {0,0,0}, //0*28, 0000, 0*28, 0000, 0*28, 0000 dis: = 4
         *         {1,0,5}, //0*28, 0001, 0*28, 0000, 0*28, 0101 dis: = 3
         *         {0,5,1}, //0*28, 0000, 0*28, 0101, 0*28, 0001 dis: = 1
         *         {1,1,4}, //0*28, 0001, 0*28, 0001, 0*28, 0100 dis: = 3
         */
        addBitTestData();

        float[] queryVector = {1.0f, 5.0f, 1.0f}; // //0*28, 0001, 0*28, 0101, 0*28, 0001
        int k = 1; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 1 is closest to the query
        }

        /**
         * update doc 2 to the nearest
         */
        Float[] updatedVector  = {1.0f, 5.0f, 1.0f};
        updateKnnDoc(INDEX_NAME, "2", FIELD_NAME, updatedVector);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId()); //Vector of DocId 3 is closest to the query
        }
    }

    public void testKNNResultsWithDeleteDoc() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId()); //Vector of DocId 2 is closest to the query
        }


        /**
         * delete the nearest doc (doc2)
         */
        deleteKnnDoc(INDEX_NAME, "2");

        knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k+1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("4", result.getDocId()); //Vector of DocId 4 is closest to the query
        }
    }
    public void testKNNResultsWithNonOptimizedIndexAndDeleteDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.negdotprod.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = 1; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("1", result.getDocId()); //Vector of DocId 1 is closest to the query
        }


        /**
         * delete the nearest doc (doc1)
         */
        deleteKnnDoc(INDEX_NAME, "1");

        knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k+1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 3 is closest to the query
        }
    }
    public void testKNNResultsWithNonOptimizedBitHammingIndexAndDeleteDoc() throws Exception {
        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.bit_hamming.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 96));
        /**
         *         {0,0,0}, //0*28, 0000, 0*28, 0000, 0*28, 0000 dis: = 4
         *         {1,0,5}, //0*28, 0001, 0*28, 0000, 0*28, 0101 dis: = 3
         *         {0,5,1}, //0*28, 0000, 0*28, 0101, 0*28, 0001 dis: = 1
         *         {1,1,4}, //0*28, 0001, 0*28, 0001, 0*28, 0100 dis: = 3
         */
        addBitTestData();

        float[] queryVector = {1.0f, 5.0f, 1.0f}; // //0*28, 0001, 0*28, 0101, 0*28, 0001
        int k = 1; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("3", result.getDocId()); //Vector of DocId 1 is closest to the query
        }


        /**
         * delete the nearest doc (doc3)
         */
        deleteKnnDoc(INDEX_NAME, "3");

        knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k+1);
        response = searchKNNIndex(INDEX_NAME, knnQueryBuilder,k);
        results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);

        assertEquals(results.size(), k);
        for(KNNResult result : results) {
            assertEquals("2", result.getDocId()); //Vector of DocId 3 is closest to the query
        }
    }


    public void testKNNResultsWithNonOptimizedBitHammingIndexBoundary() throws Exception {

        Settings settings = Settings.builder()
                .put(getKNNDefaultIndexSettings())
                .put(KNNSettings.KNN_SPACE_TYPE, SpaceTypes.bit_hamming.getValue())
                .build();
        createKnnIndex(INDEX_NAME, settings, createKnnIndexMapping(FIELD_NAME, 96));
        Float[] vector1 = {-1.0f, -2.0f, 15.0f}; //1*28, 1111, 1*28, 1110, 0*28, 1111
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector1);
        Float[] vector2 = {1.0f, 0.0f, 5.0f}; //0*28, 0001, 0*28, 0000, 0*28, 0101
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, vector2);
        Float[] vector3 = {1431655765.0f, 2147483647.0f, -1.0f}; //0101*7, 0101, 0111, 1*28, 1*28, 1111
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector3);
        forceMergeKnnIndex(INDEX_NAME);

        //doc1: 0+31+3=34, doc2: 31+0+1=32, doc3: 3=16+31+31=78
        float[] queryVector = {-1f, 0.0f, 1.0f}; // 1*28,1111, 0*28,0000, 0*28,0001
        int k = 3; //  nearest 1 neighbor
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response searchResponse = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(searchResponse.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "1", "3");
        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(actualDocids.size(), k);
        assertArrayEquals(actualDocids.toArray(), expectedDocids.toArray());
    }
    /**
     * For negative K, query builder should throw Exception
     */
    public void testNegativeK() {
        float[] vector = {1.0f, 2.0f};
        expectThrows(IllegalArgumentException.class, () -> new KNNQueryBuilder(FIELD_NAME, vector, -1));
    }

    /**
     *  For zero K, query builder should throw Exception
     */
    public void testZeroK() {
        float[] vector = {1.0f, 2.0f};
        expectThrows(IllegalArgumentException.class, () -> new KNNQueryBuilder(FIELD_NAME, vector, 0));
    }

    /**
     * K &gt; &gt; number of docs
     */
    public void testLargeK() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        addTestData();

        float[] queryVector = {1.0f, 1.0f}; // vector to be queried
        int k = KNNQueryBuilder.K_MAX; //  nearest 1 neighbor

        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder(FIELD_NAME, queryVector, k);
        Response response = searchKNNIndex(INDEX_NAME, knnQueryBuilder, k);
        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(results.size(), 4);
    }
}
