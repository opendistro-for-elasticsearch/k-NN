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

package com.amazon.opendistroforelasticsearch.knn.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.amazon.opendistroforelasticsearch.knn.index.v1736.KNNIndex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KNNIndex level caching with weight based, time based evictions. This caching helps us
 * to manage the hnsw graphs in the memory and garbage collect them after specified timeout
 * or when weightCircuitBreaker is hit.
 */
public class KNNIndexCache {

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private AtomicBoolean cacheCapacityReached;

    private static Logger logger = LogManager.getLogger(KNNIndexCache.class);
    private static KNNIndexFileListener knnIndexFileListener = null;

    private static KNNIndexCache INSTANCE;
    public Cache<String, KNNIndex> cache;

    public static void setKnnIndexFileListener(KNNIndexFileListener knnIndexFileListener) {
        KNNIndexCache.knnIndexFileListener = knnIndexFileListener;
    }

    private KNNIndexCache() {
        initCache();
    }

    /**
     * Make sure we just have one instance of cache
     * @return KNNIndexCache instance
     */
    public static synchronized KNNIndexCache getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new KNNIndexCache();
        }
        return INSTANCE;
    }

    public void initCache() {
        CacheBuilder<String, KNNIndex> cacheBuilder = CacheBuilder.newBuilder()
                                                                  .recordStats()
                                                                  .concurrencyLevel(1)
                                                                  .removalListener(k -> onRemoval(k));
        if(KNNSettings.state().getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_ENABLED)) {
            cacheBuilder.maximumWeight(KNNSettings.getCircuitBreakerLimit().getKb()).weigher((k, v) -> (int)v.getIndexSize());
        }

        if(KNNSettings.state().getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_ENABLED)) {
            /**
             * If the hnsw index is not accessed for knn.cache.item.expiry.minutes, it would be garbage collected.
             */
            long expiryTime = KNNSettings.state().getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES);
            cacheBuilder.expireAfterAccess(expiryTime, TimeUnit.MINUTES);
        }

        cacheCapacityReached = new AtomicBoolean(false);

        cache = cacheBuilder.build();
    }

    public synchronized void rebuild() {
        logger.info("KNN Cache rebuilding.");
        executor.execute(() -> {
            cache.invalidateAll();
            initCache(); }
        );
    }

    /**
     * On cache eviction, the corresponding hnsw index will be deleted from native memory.
     *
     * @param removalNotification key, value that got evicted.
     */
    private void onRemoval(RemovalNotification<String, KNNIndex> removalNotification) {
            KNNIndex knnIndex = removalNotification.getValue();

            executor.execute(() -> knnIndex.close());

            if (RemovalCause.SIZE == removalNotification.getCause()) {
                KNNSettings.state().updateCircuitBreakerSettings(true);
                setCacheCapacityReached(true);
            }
            // TODO will change below logger to debug when close to ship it
            logger.info("[KNN] Cache evicted. Key {}, Reason: {}", removalNotification.getKey()
                    ,removalNotification.getCause());
    }

    /**
     * Loads corresponding index for the given key to memory and returns the index object.
     *
     * @param key indexPath where the serialized hnsw graph is stored
     * @return KNNIndex holding the heap pointer of the loaded graph or empty if there was
     * a failure to load the
     * @throws RuntimeException if there's an unexpected failure in loading, which implies that the value for
     * the key will be both out of the cache and the underlying index will not be loaded
     */
    public KNNIndex getIndex(String key) {
        try {
            return cache.get(key, () -> loadIndex(key));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the stats of the cache
     *
     * @return Stats of the  cache
     */
    public CacheStats getStats() {
        return cache.stats();
    }

    /**
     * Returns the current weight of the cache in KiloBytes
     *
     * @return Weight of the cache
     */
    public Long getWeight() {
        return cache.asMap().values().stream().mapToLong(KNNIndex::getIndexSize).sum();
    }

    /**
     * Returns whether or not the capacity of the cache has been reached
     *
     * @return Boolean of whether cache limit has been reached
     */
    public Boolean isCacheCapacityReached() {
        return cacheCapacityReached.get();
    }

    /**
     * Sets cache capacity reached
     *
     * @param value Boolean value to set cache Capacity Reached to
     */
    public void setCacheCapacityReached(Boolean value) {
        cacheCapacityReached.set(value);
    }

    /**
     * Loads hnsw index to memory. Registers the location of the serialized graph with ResourceWatcher.
     *
     * @param indexPathUrl path for serialized hnsw graph
     * @return KNNIndex holding the heap pointer of the loaded graph
     * @throws Exception Exception could occur when registering the index path
     * to Resource watcher or if the JNI call throws
     */
    public KNNIndex loadIndex(String indexPathUrl) throws Exception {
        if(Strings.isNullOrEmpty(indexPathUrl))
            throw new IllegalStateException("indexPath is null while performing load index");
        logger.debug("Loading index on cache miss .. {}", indexPathUrl);
        Path indexPath = Paths.get(indexPathUrl);
        knnIndexFileListener.register(indexPath);
        return KNNIndex.loadIndex(indexPathUrl);
    }
}
