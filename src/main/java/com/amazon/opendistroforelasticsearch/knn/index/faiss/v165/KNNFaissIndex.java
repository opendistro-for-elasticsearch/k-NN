package com.amazon.opendistroforelasticsearch.knn.index.faiss.v165;

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndex;
import com.amazon.opendistroforelasticsearch.knn.index.KNNQueryResult;
import com.amazon.opendistroforelasticsearch.knn.index.util.FAISSLibVersion;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KNNFaissIndex extends KNNIndex implements AutoCloseable {

    public static FAISSLibVersion VERSION = FAISSLibVersion.VFAISS_165;
    static {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.loadLibrary(FAISSLibVersion.VFAISS_165.indexLibraryVersion());
                return null;
            }
        });
        initLibrary();
    }

    private volatile boolean isClosed = false;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final long indexPointer;
    private final long indexSize;

    private KNNFaissIndex(final long indexPointer, final long indexSize) {
        this.indexPointer = indexPointer;
        this.indexSize = indexSize;
    }
    public KNNFaissIndex() {
        this.indexPointer = 0;
        this.indexSize = 0;
    }

    @Override
    public long getIndexSize() {
        return this.indexSize;
    }

    @Override
    public KNNQueryResult[] queryIndex(final float[] query, final int k) throws RuntimeException {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        KNNCounter.GRAPH_QUERY_REQUESTS.increment();
        try {
            if (this.isClosed) {
                throw new IOException("Index is already closed");
            }
            final long indexPointer = this.indexPointer;
            return AccessController.doPrivileged(
                    new PrivilegedAction<KNNQueryResult[]>() {
                        public KNNQueryResult[] run() {
                            return queryIndex(indexPointer, query, k);
                        }
                    }
            );

        } catch (Exception ex) {
            KNNCounter.GRAPH_QUERY_ERRORS.increment();
            throw new RuntimeException("Unable to query the index: " + ex);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void saveIndex(int[] ids, float[][] data, String indexPath,
                          String[] algoParams, String spaceType,
                          KNNEngine engine) throws  RuntimeException{
        if (engine != KNNEngine.FAISS) {
            throw new RuntimeException("Index Engine: " +
                    engine.getKnnEngineName() +
                    " is not support in" + KNNEngine.FAISS);
        }
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                         saveIndex(ids, data, indexPath, algoParams, spaceType);
                         return null;
                    }
                }
        );
    }

    @Override
    public void close() {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        // Autocloseable documentation recommends making close idempotent. We don't expect to doubly close
        // but this will help prevent a crash in that situation.
        if (this.isClosed) {
            return;
        }
        try {
            gc(this.indexPointer);
        } finally {
            this.isClosed = true;
            writeLock.unlock();
        }
    }

    /**
     * Loads the knn index to memory for querying the neighbours
     *
     * @param indexPath path where the hnsw index is stored
     * @param algoParams hnsw algorithm parameters
     * @param spaceType space type of the index
     * @return knn index that can be queried for k nearest neighbours
     */
    public static KNNFaissIndex loadIndex(String indexPath, final String[] algoParams, final String spaceType) {
        long fileSize = computeFileSize(indexPath);
        long indexPointer = init(indexPath, algoParams, spaceType);
        return new KNNFaissIndex(indexPointer, fileSize);
    }

    /**
     * determines the size of the hnsw index on disk
     * @param indexPath absolute path of the index
     *
     */
    private static long computeFileSize(String indexPath) {
        if (indexPath == null || indexPath.isEmpty()) {
            return 0;
        }
        File file = new File(indexPath);
        if (!file.exists() || !file.isFile()) {
            return 0;
        }

        return file.length() / 1024 + 1;
    }

    // Builds index and writes to disk (no index pointer escapes).
    public static native void saveIndex(int[] ids, float[][] data, String indexPath, String[] algoParams, String spaceType);

    // Queries index (thread safe with other readers, blocked by write lock)
    private static native KNNQueryResult[] queryIndex(long indexPointer, float[] query, int k);


    // Loads index and returns pointer to index
    private static native long init(String indexPath, String[] algoParams, String spaceType);

    // Deletes memory pointed to by index pointer (needs write lock)
    private static native void gc(long indexPointer);

    private static native void initLibrary();
}
