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

package com.amazon.opendistroforelasticsearch.knn.index.nmslib.v2011;

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndex;
import com.amazon.opendistroforelasticsearch.knn.index.KNNQueryResult;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.amazon.opendistroforelasticsearch.knn.index.util.NmsLibVersion;

import java.security.AccessController;
import java.security.PrivilegedAction;

import static com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter.GRAPH_QUERY_REQUESTS;

/**
 * JNI layer to communicate with the nmslib
 * This class refers to the nms library build with version tag 2.0.11
 * See <a href="https://github.com/nmslib/nmslib/tree/v2.0.8">tag2.0.11</a>
 */
public class KNNNmsLibIndex extends KNNIndex {
    public static NmsLibVersion VERSION = NmsLibVersion.VNMSLIB_2011;

    static {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.loadLibrary(NmsLibVersion.VNMSLIB_2011.indexLibraryVersion());
            return null;
        });
        initLibrary();
    }

    /**
     * Loads the knn index to memory for querying the neighbours
     *
     * @param indexPath path where the hnsw index is stored
     * @param algoParams hnsw algorithm parameters
     * @param spaceType space type of the index
     * @return knn index that can be queried for k nearest neighbours
     */
    public static KNNNmsLibIndex loadIndex(String indexPath, final String[] algoParams, final SpaceType spaceType) {
        long fileSize = computeFileSize(indexPath);
        long indexPointer = init(indexPath, algoParams, spaceType.getValue());
        return new KNNNmsLibIndex(indexPointer, fileSize, spaceType);
    }

    private KNNNmsLibIndex(final long indexPointer, final long indexSize, final SpaceType spaceType) {
        super(indexPointer, indexSize, spaceType);
    }

    /*
     * Wrappers around Jni functions
     */
    protected KNNQueryResult[] queryIndexJniWrapper(long indexPointer, float[] query, int k) {
        GRAPH_QUERY_REQUESTS.increment();
        return AccessController.doPrivileged(
                (PrivilegedAction<KNNQueryResult[]>) () -> queryIndex(indexPointer, query, k)
        );
    }

    protected void gcJniWrapper(long indexPointer) {
        gc(indexPointer);
    }


    // JNI FUNCTIONS
    // Builds index and writes to disk (no index pointer escapes).
    public static native void saveIndex(int[] ids, float[][] data, String indexPath, String[] algoParams,
                                        String spaceType, String method);

    // Queries index (thread safe with other readers, blocked by write lock)
    private static native KNNQueryResult[] queryIndex(long indexPointer, float[] query, int k);

    // Loads index and returns pointer to index
    private static native long init(String indexPath, String[] algoParams, String spaceType);

    // Deletes memory pointed to by index pointer (needs write lock)
    private static native void gc(long indexPointer);

    // Calls nmslib's initLibrary function
    private static native void initLibrary();
}
