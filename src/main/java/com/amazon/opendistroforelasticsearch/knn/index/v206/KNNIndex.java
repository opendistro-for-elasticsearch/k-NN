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

package com.amazon.opendistroforelasticsearch.knn.index.v206;

import com.amazon.opendistroforelasticsearch.knn.index.KNNQueryResult;
import com.amazon.opendistroforelasticsearch.knn.index.util.NmsLibVersion;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * JNI layer to communicate with the nmslib
 * This class refers to the nms library build with version tag 2.0.6
 * See <a href="https://github.com/nmslib/nmslib/tree/v2.0.6">tag2.0.6</a>
 */
public class KNNIndex implements AutoCloseable {
    public static NmsLibVersion VERSION = NmsLibVersion.V206;

    static {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            public Void run() {
                System.loadLibrary(NmsLibVersion.V206.indexLibraryVersion());
                return null;
            }
        });
        initLibrary();
    }

    private volatile boolean isClosed = false;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final long indexPointer;
    private final long indexSize;

    private KNNIndex(final long indexPointer, final long indexSize) {
        this.indexPointer = indexPointer;
        this.indexSize = indexSize;
    }

    /**
     * This function is useful in computing the weight for caching. File sizes are stored in KiloBytes to prevent an
     * Integer Overflow. The Guava Cache weigh method returns an int. The max size of a Java int is 2,147,483,647. So,
     * a 2GB file, would lead to an overflow. With KB, however, 2,147,483,647 KB = 1.99 TB. So, it would take a 2 TB
     * file to produce an Integer Overflow.
     *
     * @return size of the hnsw index on the disk in KB.
     */
    public long getIndexSize() {
        return this.indexSize;
    }

    public KNNQueryResult[] queryIndex(final float[] query, final int k) throws IOException {
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
    public static KNNIndex loadIndex(String indexPath, final String[] algoParams, final String spaceType) {
        long fileSize = computeFileSize(indexPath);
        long indexPointer = init(indexPath, algoParams, spaceType);
        return new KNNIndex(indexPointer, fileSize);
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

    // Calls nmslib's initLibrary function: https://github.com/nmslib/nmslib/blob/v2.0.6/similarity_search/include/init.h#L27
    private static native void initLibrary();
}
