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

package org.elasticsearch.index.knn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.index.knn.v1736.KNNIndex;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public class KNNJNIIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(KNNJNIIT.class);

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

        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath);
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

        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath);
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};

        KNNQueryResult[] results = AccessController.doPrivileged(
                new PrivilegedAction<KNNQueryResult[]>() {
                    public KNNQueryResult[] run() {
                        KNNIndex index = KNNIndex.loadIndex(indexPath.toString());
                        logger.info(index.getIndex());
                        return index.queryIndex(queryVector, 30);
                    }
                }
        );

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(result -> result.getId(), result -> result.getScore()));
        logger.info(scores);

        assertEquals(results.length, 3);
        /*
         * scores are evaluated using Euclidean distance. Distance of the documents with
         * respect to query vector are as follows
         * doc0 = 11.224972, doc1 = 3.7416575,  doc2 = 19.131126
         * Nearest neighbor is doc1 then doc0 then doc2
         */
        assertEquals(scores.get(0), 11.224972, 0.1);
        assertEquals(scores.get(1), 3.7416575, 0.1);
        assertEquals(scores.get(2), 19.131126, 0.1);
        dir.close();
    }

    public void testAssertExceptionFromJni() throws Exception {

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        /***
         * Trying to load index which did not exist. This results in Runtime Error in nmslib.
         * Making sure c++ exceptions are casted to java Exception to avoid ES process crash
         */
        expectThrows(Exception.class, () ->
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex index = KNNIndex.loadIndex(indexPath.toString());
                        return null;
                    }
                }
        ));
        dir.close();
    }
}