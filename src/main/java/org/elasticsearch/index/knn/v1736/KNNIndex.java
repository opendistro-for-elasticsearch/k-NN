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

package org.elasticsearch.index.knn.v1736;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.knn.KNNQueryResult;
import org.elasticsearch.index.knn.util.NmsLibVersion;
import org.elasticsearch.index.knn.KNNIndexCache;

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

    private static Logger logger = LogManager.getLogger(KNNIndex.class);

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
     * This function is useful in computing the weight for caching
     * @return size of the index on the disk
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
        if (!Strings.isNullOrEmpty(indexPath)) {
            File file = new File(indexPath);
            if (!file.exists() || !file.isFile()) {
                logger.debug("File {} deleted. Skipping ", indexPath);
                setIndexSize(0);
            } else {
                setIndexSize(file.length());
            }
        }
    }

    public static native void saveIndex(int[] ids, float[][] data, String indexPath);

    public native KNNQueryResult[] queryIndex(float[] query, int k);

    /**
     * Loads the knn index to memory for querying the neighbours
     *
     * @param indexPath path where the hnsw index is stored
     * @return knn index that can be queried for k nearest neighbours
     */
    public static KNNIndex loadIndex(String indexPath) {
        KNNIndex index = new KNNIndex();
        index.init(indexPath);
        if (KNNIndexCache.weightCircuitBreakerEnabled)
            // File size is treated as weight
            index.computeFileSize(indexPath);
        return index;
    }

    public native void init(String indexPath);

    public native void gc();
}
