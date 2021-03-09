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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Encapsulates functionality to interface with an underlying k-NN Engine
 */
public abstract class KNNIndex implements AutoCloseable {

    protected volatile boolean isClosed = false;
    protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    protected final long indexPointer;
    protected final long indexSize;

    protected KNNIndex(final long indexPointer, final long indexSize) {
        this.indexPointer = indexPointer;
        this.indexSize = indexSize;
    }

    /**
     * Query the kNN Index
     * @param query query vector
     * @param k top k nearest
     * @return array of @KNNQueryResult
     */
    public KNNQueryResult[] queryIndex(final float[] query, final int k) throws RuntimeException {
        Lock readLock = readWriteLock.readLock();
        readLock.lock();
        try {
            if (this.isClosed) {
                throw new IOException("Index is already closed");
            }
            final long indexPointer = this.indexPointer;
            return queryIndexJniWrapper(indexPointer, query, k);

        } catch (Exception ex) {
            throw new RuntimeException("Unable to query the index: " + ex);
        } finally {
            readLock.unlock();
        }
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

    public void close() {
        Lock writeLock = readWriteLock.writeLock();
        writeLock.lock();
        // Autocloseable documentation recommends making close idempotent. We don't expect to doubly close
        // but this will help prevent a crash in that situation.
        if (this.isClosed) {
            return;
        }
        try {
            gcJniWrapper(this.indexPointer);
        } finally {
            this.isClosed = true;
            writeLock.unlock();
        }
    }

    /**
     * determines the size of the engine index on disk
     * @param indexPath absolute path of the index
     *
     */
    protected static long computeFileSize(String indexPath) {
        if (indexPath == null || indexPath.isEmpty()) {
            return 0;
        }
        File file = new File(indexPath);
        if (!file.exists() || !file.isFile()) {
            return 0;
        }

        return file.length() / 1024 + 1;
    }


    /*
     * Wrappers around Jni functions
     */
    protected abstract KNNQueryResult[] queryIndexJniWrapper(long indexPointer, float[] query, int k);
    protected abstract void gcJniWrapper(long indexPointer);
}
