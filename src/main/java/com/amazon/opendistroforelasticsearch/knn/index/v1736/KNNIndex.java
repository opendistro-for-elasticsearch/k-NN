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

package com.amazon.opendistroforelasticsearch.knn.index.v1736;

import com.amazon.opendistroforelasticsearch.knn.index.KNNQueryResult;
import com.amazon.opendistroforelasticsearch.knn.index.util.NmsLibVersion;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JNI layer to communicate with the nmslib
 * This class refers to the nms library build with version tag 1.7.3.6
 * See <a href="https://github.com/nmslib/nmslib/tree/v1.7.3.6">tag1.7.3.6</a>
 */
public class KNNIndex {
    public static NmsLibVersion VERSION = NmsLibVersion.V1736;
    static {
        System.loadLibrary(NmsLibVersion.V1736.indexLibraryVersion());
    }

    public AtomicBoolean isDeleted = new AtomicBoolean(false);

    private long index;
    private long indexSize;

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public void setIndexSize(long indexSize) {
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

    /**
     * determines the size of the hnsw index on disk
     * @param indexPath absolute path of the index
     *
     */
    public void computeFileSize(String indexPath) {
        if (indexPath != null && !indexPath.isEmpty()) {
            File file = new File(indexPath);
            if (!file.exists() || !file.isFile()) {
                setIndexSize(0);
            } else {
                setIndexSize(file.length()/1024 + 1); // convert to KB and round up
            }
        }
    }

    public static native void saveIndex(int[] ids, float[][] data, String indexPath, String[] algoParams);

    public native KNNQueryResult[] queryIndex(float[] query, int k, String[] algoParams);

    /**
     * Loads the knn index to memory for querying the neighbours
     *
     * @param indexPath path where the hnsw index is stored
     * @return knn index that can be queried for k nearest neighbours
     */
    public static KNNIndex loadIndex(String indexPath) {
        KNNIndex index = new KNNIndex();
        index.init(indexPath);
        index.computeFileSize(indexPath); // File size is treated as weight
        return index;
    }

    public native void init(String indexPath);

    public native void gc();
}
