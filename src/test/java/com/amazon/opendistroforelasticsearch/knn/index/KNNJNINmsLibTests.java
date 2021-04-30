/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.nmslib.v2011.KNNNmsLibIndex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;

public class KNNJNINmsLibTests extends KNNTestCase {
    private static final Logger logger = LogManager.getLogger(KNNJNINmsLibTests.class);

    public void testCreateHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {1.0f, 2.0f, 3.0f, 4.0f},
                {5.0f, 6.0f, 7.0f, 8.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        String[] algoParams = {};
                AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNNmsLibIndex.save(docs, vectors, indexPath, algoParams, SpaceType.L2.getValue(),
                                METHOD_HNSW);
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy.hnsw"));
        dir.close();
    }

    public void testQueryHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {5.0f, 6.0f, 7.0f, 8.0f},
                {1.0f, 2.0f, 3.0f, 4.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        String[] algoParams = {};
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNNmsLibIndex.save(docs, vectors, indexPath, algoParams, SpaceType.L2.getValue(),
                                METHOD_HNSW);
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};
        String[] algoQueryParams = {"efSearch=20"};

        final KNNNmsLibIndex knnNmsLibIndex = KNNNmsLibIndex.load(indexPath, algoQueryParams, SpaceType.L2);
        final KNNQueryResult[] results = knnNmsLibIndex.query(queryVector, 30);

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(KNNQueryResult::getId, KNNQueryResult::getScore));
        logger.info(scores);

        assertEquals(results.length, 3);
        /*
         * scores are evaluated using Euclidean distance. Distance of the documents with
         * respect to query vector are as follows
         * doc0 = 126, doc1 = 14,  doc2 = 366
         * Nearest neighbor is doc1 then doc0 then doc2
         */
        assertEquals(126.0, scores.get(0), 0.001);
        assertEquals(14.0, scores.get(1), 0.001);
        assertEquals(366.0, scores.get(2), 0.001);
        dir.close();
    }

    public void testAddAndQueryHnswIndexCosineSimil() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
            {1.0f, -1.0f},
            {1.0f, 0.0f},
            {1.0f, 1.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
            String.format("%s.hnsw", segmentName)).toString();

        String[] algoParams = {};
        AccessController.doPrivileged(
            new PrivilegedAction<Void>() {
                public Void run() {
                    KNNNmsLibIndex.save(docs, vectors, indexPath, algoParams, SpaceType.COSINESIMIL.getValue(),
                            METHOD_HNSW);
                    return null;
                }
            }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {2.0f, -2.0f};
        String[] algoQueryParams = {"efSearch=20"};

        final KNNNmsLibIndex knnNmsLibIndex = KNNNmsLibIndex.load(indexPath, algoQueryParams, SpaceType.COSINESIMIL);
        final KNNQueryResult[] results = knnNmsLibIndex.query(queryVector, 30);

        Map<Integer, Float> scores = Arrays.stream(results).collect(
            Collectors.toMap(KNNQueryResult::getId, KNNQueryResult::getScore));
        logger.info(scores);

        assertEquals(results.length, 3);
        /*
         * scores are evaluated using cosine similarity. Distance of the documents with
         * respect to query vector are as follows
         * doc0 = 0.0, doc1 = 0.29289,  doc2 = 1.0
         * Nearest neighbor is doc1 then doc0 then doc2
         */
        assertEquals(scores.get(0), 0.0, 1e-4);
        assertEquals(scores.get(1), 0.292893, 1e-4);
        assertEquals(scores.get(2), 1.0, 1e-4);
        dir.close();
    }

    public void testAssertExceptionFromJni() throws Exception {

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();


         //Trying to load index which did not exist. This results in Runtime Error in nmslib.
         //Making sure c++ exceptions are casted to java Exception to avoid ES process crash
        expectThrows(Exception.class, () ->
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNNmsLibIndex index = KNNNmsLibIndex.load(indexPath, new String[] {}, SpaceType.L2);
                        return null;
                    }
                }
        ));
        dir.close();
    }

    public void testQueryHnswIndexWithValidAlgoParams() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {5.0f, 6.0f, 7.0f, 8.0f},
                {1.0f, 2.0f, 3.0f, 4.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String indexPath = getIndexPath(dir);


         //Passing valid algo params should not fail the graph construction.
        String[] algoIndexParams = {"M=32","efConstruction=200"};
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNNmsLibIndex.save(docs, vectors, indexPath, algoIndexParams, SpaceType.L2.getValue(),
                                METHOD_HNSW);
                        return null;
                    }
                }
        );


        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};
        String[] algoQueryParams = {"efSearch=200"};

        final KNNNmsLibIndex index = KNNNmsLibIndex.load(indexPath, algoQueryParams, SpaceType.L2);
        final KNNQueryResult[] results = index.query(queryVector, 30);

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(KNNQueryResult::getId, KNNQueryResult::getScore));
        logger.info(scores);

        assertEquals(results.length, 3);
        /*
         * scores are evaluated using Euclidean distance. Distance of the documents with
         * respect to query vector are as follows
         * doc0 = 126, doc1 = 14,  doc2 = 366
         * Nearest neighbor is doc1 then doc0 then doc2
         */
        assertEquals(126.0, scores.get(0), 0.001);
        assertEquals(14.0, scores.get(1), 0.001);
        assertEquals(366.0, scores.get(2), 0.001);
        dir.close();
    }

    private String getIndexPath(Directory dir ) {
        String segmentName = "_dummy1";
        return Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();
    }
}
