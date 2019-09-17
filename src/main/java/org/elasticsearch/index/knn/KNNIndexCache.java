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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.knn.v1736.KNNIndex;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.ToLongBiFunction;

/**
 * KNNIndex level caching with weight based, time based evictions. This caching helps us
 * to manage the hnsw graphs in the memory and garbage collect them after specified timeout
 * or when weightCircuitBreaker is hit.
 */
public class KNNIndexCache implements RemovalListener<String, KNNIndex>, Releasable {

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private static Logger logger = LogManager.getLogger(KNNIndexCache.class);
    private static KNNIndexFileListener knnIndexFileListener = null;

    public static boolean weightCircuitBreakerEnabled = false;
    public static boolean timestampEnabled = true;
    public Cache<String, KNNIndex> cache;

    // TODO Expose these as Elasticsearch settings
    private final long sizeInBytes = 1 * 1024 * 1024 * 1024; //1GB

    public static void setKnnIndexFileListener(KNNIndexFileListener knnIndexFileListener) {
        KNNIndexCache.knnIndexFileListener = knnIndexFileListener;
    }

    public KNNIndexCache() {
        CacheBuilder<String, KNNIndex> cacheBuilder = CacheBuilder.<String, KNNIndex>builder()
                                                                 .removalListener(this);
        if (weightCircuitBreakerEnabled) {
            cacheBuilder.setMaximumWeight(sizeInBytes).weigher(new KNNIndexWeight());
        }

        if(timestampEnabled) {
            /**
             * If the hnsw index is not accessed for 90 minutes it will be removed from memory
             * This time out will be later exposed as dynamic setting.
             */
            cacheBuilder.setExpireAfterAccess(TimeValue.timeValueMinutes(90));
        }
        cache = cacheBuilder.build();
    }

    /**
     * Invalidates the cache and shutdown the executor
     */
    @Override
    public void close() {
        executor.execute(() -> cache.invalidateAll());
        executor.shutdown();
    }

    /**
     * On cache eviction, the corresponding hnsw index will be deleted from heap.
     *
     * @param removalNotification key, value that got evicted.
     */
    @Override
    public void onRemoval(RemovalNotification<String, KNNIndex> removalNotification) {
        try {
            logger.debug("[KNN] Cache evicted. Key {}, Reason: {}", removalNotification.getKey()
                                 ,removalNotification.getRemovalReason());
            KNNIndex knnIndex = removalNotification.getValue();
            // This flag is to ensure, callers already holding the object do not query if index
            // is deleted
            knnIndex.isDeleted.set(true);
            executor.execute(() -> knnIndex.gc());
        } catch(Exception ex) {
            logger.error("Exception occured while performing gc for hnsw index " + removalNotification.getKey());
        }
    }

    /**
     * Adds entry to the cache
     *
     * @param key indexpath
     * @param value heap pointer of the index in memory
     */
    public void addEntry(String key, KNNIndex value) {
        if(Strings.isNullOrEmpty(key))
            throw new IllegalStateException("indexPath should be valid key");
        cache.put(key, value);
    }

    /**
     * Loads corresponding index for the given key to memory and returns the index object.
     *
     * @param key indexPath where the serialized hnsw graph is stored
     * @return KNNIndex holding the heap pointer of the loaded graph
     */
    public KNNIndex getIndex(String key) {
        try {
            return cache.computeIfAbsent(key, indexPathUrl -> computeIndex(indexPathUrl));
        } catch (ExecutionException e) {
            logger.error("Exception occured while computing the index. Skipped Adding to cache");
        }
        return null;
    }

    /**
     * Loads hnsw index to memory. Registers the location of the serialized graph with ResourceWatcher.
     *
     * @param indexPathUrl path for serialized hnsw graph
     * @return KNNIndex holding the heap pointer of the loaded graph
     * @throws Exception Exception could occur when registering the index path
     * to Resource watcher
     */
    public KNNIndex computeIndex(String indexPathUrl) throws Exception {
        if(Strings.isNullOrEmpty(indexPathUrl))
            throw new IllegalStateException("indexPath is null while performing load index");
        Path indexPath = Paths.get(indexPathUrl);
        knnIndexFileListener.register(indexPath);
        return KNNIndex.loadIndex(indexPathUrl);
    }

    /**
     * Class to compute the weight of the cache entry
     */
    class KNNIndexWeight implements ToLongBiFunction<String, KNNIndex> {
        @Override
        public long applyAsLong(String s, KNNIndex knnIndex) {
            return knnIndex.getIndexSize();
        }
    }
}

