package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.faiss.v165.KNNFaissIndex;
import com.amazon.opendistroforelasticsearch.knn.index.nmslib.v2011.KNNNmsLibIndex;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class KNNJNITests extends KNNTestCase {

    private static final Logger logger = LogManager.getLogger(KNNJNINmsLibTests.class);

    public void testCreateMixedEngineHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {1.0f, 2.0f, 3.0f, 4.0f},
                {5.0f, 6.0f, 7.0f, 8.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy";
        KNNEngine faissEngine = KNNEngine.FAISS;
        KNNEngine nmslibEngine = KNNEngine.NMSLIB;
        {
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                    String.format("%s_%s.hnsw", segmentName, nmslibEngine.knnEngineName)).toString();
            KNNIndex index = new KNNNmsLibIndex();
            String[] algoParams = {};
            index.saveIndex(docs, vectors, indexPath, algoParams, "l2", nmslibEngine);
        }
        {
            String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                    String.format("%s_%s.hnsw", segmentName, faissEngine.knnEngineName)).toString();
            KNNIndex index = new KNNFaissIndex();
            String[] algoParams = {};
            index.saveIndex(docs, vectors, indexPath, algoParams, "l2", faissEngine);
        }
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_NMSLIB.hnsw"));
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_FAISS.hnsw"));
        dir.close();
    }

    public void testQueryMixedEngineHnswIndex() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {5.0f, 6.0f, 7.0f, 8.0f},
                {1.0f, 2.0f, 3.0f, 4.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy";
        KNNEngine faissEngine = KNNEngine.FAISS;
        KNNEngine nmslibEngine = KNNEngine.NMSLIB;
        String indexNmsLibPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s_%s.hnsw", segmentName, nmslibEngine.knnEngineName)).toString();
        String indexFaissPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s_%s.hnsw", segmentName, faissEngine.knnEngineName)).toString();
        {

            KNNIndex index = new KNNNmsLibIndex();
            String[] algoParams = {};
            index.saveIndex(docs, vectors, indexNmsLibPath, algoParams, "l2", nmslibEngine);
        }
        {
            KNNIndex index = new KNNFaissIndex();
            String[] algoParams = {};
            index.saveIndex(docs, vectors, indexFaissPath, algoParams, "l2", faissEngine);
        }
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_NMSLIB.hnsw"));
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_FAISS.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};
        String[] algoQueryParams = {"efSearch=20"};

        {
            final KNNNmsLibIndex knnNmsLibIndex = KNNNmsLibIndex.loadIndex(indexNmsLibPath, algoQueryParams, "l2");
            final KNNQueryResult[] results = knnNmsLibIndex.queryIndex(queryVector, 30);
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> result.getScore()));
            logger.info(scores);
            assertEquals(results.length, 3);
            assertEquals(126.0, scores.get(0), 0.001);
            assertEquals(14.0, scores.get(1), 0.001);
            assertEquals(366.0, scores.get(2), 0.001);
        }
        {
            final KNNFaissIndex knnFaissLibIndex = KNNFaissIndex.loadIndex(indexFaissPath, algoQueryParams, "l2");
            final KNNQueryResult[] results = knnFaissLibIndex.queryIndex(queryVector, 30);
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> result.getScore()));
            logger.info(scores);
            assertEquals(results.length, 3);
            assertEquals(126.0, scores.get(0), 0.001);
            assertEquals(14.0, scores.get(1), 0.001);
            assertEquals(366.0, scores.get(2), 0.001);
        }
        dir.close();
    }

    public void testQueryMixedEngineHnswIndexWithWrongEngine() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {5.0f, 6.0f, 7.0f, 8.0f},
                {1.0f, 2.0f, 3.0f, 4.0f},
                {9.0f, 10.0f, 11.0f, 12.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy";
        KNNEngine faissEngine = KNNEngine.FAISS;
        KNNEngine nmslibEngine = KNNEngine.NMSLIB;
        String indexNmsLibPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s_%s.hnsw", segmentName, nmslibEngine.knnEngineName)).toString();
        String indexFaissPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s_%s.hnsw", segmentName, faissEngine.knnEngineName)).toString();
        {

            KNNIndex index = new KNNNmsLibIndex();
            String[] algoParams = {};
            //Index With Wrong Engine, need throw runtime exception
            expectThrows(RuntimeException.class,
                    () -> index.saveIndex(docs, vectors, indexNmsLibPath, algoParams, "l2", faissEngine));
            index.saveIndex(docs, vectors, indexNmsLibPath, algoParams, "l2", nmslibEngine);
        }
        {
            KNNIndex index = new KNNFaissIndex();
            String[] algoParams = {};
            //Index With Wrong Engine, need throw runtime exception
            expectThrows(RuntimeException.class,
                    () -> index.saveIndex(docs, vectors, indexNmsLibPath, algoParams, "l2", nmslibEngine));
            index.saveIndex(docs, vectors, indexFaissPath, algoParams, "l2", faissEngine);
        }
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_NMSLIB.hnsw"));
        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy_FAISS.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};
        String[] algoQueryParams = {"efSearch=20"};

        {
            //Load Index With Wrong Engine, need throw Exception
            expectThrows(Exception.class, () -> KNNFaissIndex.loadIndex(indexNmsLibPath, algoQueryParams, "l2"));
            final KNNNmsLibIndex knnNmsLibIndex = KNNNmsLibIndex.loadIndex(indexNmsLibPath, algoQueryParams, "l2");
            final KNNQueryResult[] results = knnNmsLibIndex.queryIndex(queryVector, 30);
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> result.getScore()));
            logger.info(scores);
            assertEquals(results.length, 3);
            assertEquals(126.0, scores.get(0), 0.001);
            assertEquals(14.0, scores.get(1), 0.001);
            assertEquals(366.0, scores.get(2), 0.001);
        }
        {
            //Load Index With Wrong Engine, need throw Exception
            expectThrows(Exception.class, () -> KNNNmsLibIndex.loadIndex(indexFaissPath, algoQueryParams, "l2"));
            final KNNFaissIndex knnFaissLibIndex = KNNFaissIndex.loadIndex(indexFaissPath, algoQueryParams, "l2");
            final KNNQueryResult[] results = knnFaissLibIndex.queryIndex(queryVector, 30);
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> result.getScore()));
            logger.info(scores);
            assertEquals(results.length, 3);
            assertEquals(126.0, scores.get(0), 0.001);
            assertEquals(14.0, scores.get(1), 0.001);
            assertEquals(366.0, scores.get(2), 0.001);
        }
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

        /**
         * Passing valid algo params should not fail the graph construction.
         */
        String[] algoIndexParams = {"M=32","efConstruction=200"};
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath, algoIndexParams, "l2");
                        return null;
                    }
                }
        );


        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {1.0f, 1.0f, 1.0f, 1.0f};
        String[] algoQueryParams = {"efSearch=200"};

        final KNNIndex index = KNNIndex.loadIndex(indexPath, algoQueryParams, "l2");
        final KNNQueryResult[] results = index.queryIndex(queryVector, 30);

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(result -> result.getId(), result -> result.getScore()));
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

    public void testAddAndQueryHnswIndexInnerProd() throws Exception {
        int[] docs = {0, 1, 2};

        float[][] vectors = {
                {1.0f, -1.0f},
                {-1.0f, 1.0f},
                {0.0f, 0.0f}
        };

        Directory dir = newFSDirectory(createTempDir());
        String segmentName = "_dummy1";
        String indexPath = Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();

        String[] algoParams = {};
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        KNNIndex.saveIndex(docs, vectors, indexPath, algoParams, SpaceTypes.inner_product.getValue());
                        return null;
                    }
                }
        );

        assertTrue(Arrays.asList(dir.listAll()).contains("_dummy1.hnsw"));

        float[] queryVector = {2.0f, -2.0f};
        String[] algoQueryParams = {"efSearch=20"};

        final KNNIndex knnIndex = KNNIndex.loadIndex(indexPath, algoQueryParams, SpaceTypes.inner_product.getValue());
        final KNNQueryResult[] results = knnIndex.queryIndex(queryVector, 30);

        Map<Integer, Float> scores = Arrays.stream(results).collect(
                Collectors.toMap(result -> result.getId(), result -> result.getScore()));
        logger.info(scores);

        assertEquals(results.length, 3);
        /*
         * scores are evaluated using negative dot product similarity. Distance of the documents with
         * respect to query vector are as follows
         * doc0 = -4.0, doc1 = 4.0,  doc2 = 0.0
         * Nearest neighbor is doc1 then doc0 then doc2
         */
        assertEquals(scores.get(0), -4.0, 1e-4);
        assertEquals(scores.get(1), 4.0, 1e-4);
        assertEquals(scores.get(2), 0.0, 1e-4);
        dir.close();
    }

    private String getIndexPath(Directory dir ) {
        String segmentName = "_dummy1";
        return Paths.get(((FSDirectory) (FilterDirectory.unwrap(dir))).getDirectory().toString(),
                String.format("%s.hnsw", segmentName)).toString();
    }
}
