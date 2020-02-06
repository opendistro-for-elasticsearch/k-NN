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

import com.amazon.opendistroforelasticsearch.knn.index.v1736.KNNIndex;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.WatcherHandle;

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
    private static Logger logger = LogManager.getLogger(KNNIndexCache.class);

    private static KNNIndexCache INSTANCE;

    private Cache<String, KNNIndexCacheEntry> cache;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private AtomicBoolean cacheCapacityReached;
    private ResourceWatcherService resourceWatcherService;

    private KNNIndexCache() {
        initCache();
    }

    public static void setResourceWatcherService(final ResourceWatcherService resourceWatcherService) {
        getInstance().resourceWatcherService = resourceWatcherService;
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

    private void initCache() {
        CacheBuilder<String, KNNIndexCacheEntry> cacheBuilder = CacheBuilder.newBuilder()
                .recordStats()
                .concurrencyLevel(1)
                .removalListener(k -> onRemoval(k));
        if(KNNSettings.state().getSettingValue(KNNSettings.KNN_MEMORY_CIRCUIT_BREAKER_ENABLED)) {
            cacheBuilder.maximumWeight(KNNSettings.getCircuitBreakerLimit().getKb()).weigher((k, v) -> (int)v.getKnnIndex().getIndexSize());
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
    private void onRemoval(RemovalNotification<String, KNNIndexCacheEntry> removalNotification) {
        KNNIndexCacheEntry knnIndexCacheEntry = removalNotification.getValue();

        knnIndexCacheEntry.getFileWatcherHandle().stop();

        executor.execute(() -> knnIndexCacheEntry.getKnnIndex().close());

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
     * @param algoParams hnsw algorithm parameters
     * @return KNNIndex holding the heap pointer of the loaded graph
     */
    public KNNIndex getIndex(String key, final String[] algoParams) {
        try {
            final KNNIndexCacheEntry knnIndexCacheEntry = cache.get(key, () -> loadIndex(key, algoParams));
            return knnIndexCacheEntry.getKnnIndex();
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
     * @return Weight of the cache in kilobytes
     */
    public Long getWeightInKilobytes() {
        return cache.asMap().values().stream().map(KNNIndexCacheEntry::getKnnIndex).mapToLong(KNNIndex::getIndexSize).sum();
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
     * @param algoParams hnsw algorithm parameters
     * @return KNNIndex holding the heap pointer of the loaded graph
     * @throws Exception Exception could occur when registering the index path
     * to Resource watcher or if the JNI call throws
     */
    public KNNIndexCacheEntry loadIndex(String indexPathUrl, final String[] algoParams) throws Exception {
        if(Strings.isNullOrEmpty(indexPathUrl))
            throw new IllegalStateException("indexPath is null while performing load index");
        logger.debug("Loading index on cache miss .. {}", indexPathUrl);
        Path indexPath = Paths.get(indexPathUrl);
        FileWatcher fileWatcher = new FileWatcher(indexPath);
        fileWatcher.addListener(KNN_INDEX_FILE_DELETED_LISTENER);

        // Calling init() on the FileWatcher will bootstrap initial state that indicates whether or not the file
        // is present. If it is not present at time of init(), then KNNIndex.loadIndex will fail and we won't cache
        // the entry
        fileWatcher.init();

        final KNNIndex knnIndex = KNNIndex.loadIndex(indexPathUrl, algoParams);

        // TODO verify that this is safe - ideally we'd explicitly ensure that the FileWatcher is only checked
        // after the guava cache has finished loading the key to avoid a race condition where the watcher
        // causes us to invalidate an entry before the key has been fully loaded.
        final WatcherHandle<FileWatcher> watcherHandle = resourceWatcherService.add(fileWatcher);

        return new KNNIndexCacheEntry(knnIndex, watcherHandle);
    }

    /**
     * KNNIndexCacheEntry is the value type for entries in the cache held by {@link KNNIndexCache}.
     * It holds a reference to both the KNNIndex and the WatcherHandle so that each can be cleaned up
     * upon expiration of the cache.
     */
    private static class KNNIndexCacheEntry {
        private final KNNIndex knnIndex;
        private final WatcherHandle<FileWatcher> fileWatcherHandle;

        private KNNIndexCacheEntry(final KNNIndex knnIndex, final WatcherHandle<FileWatcher> fileWatcherHandle) {
            this.knnIndex = knnIndex;
            this.fileWatcherHandle = fileWatcherHandle;
        }

        private KNNIndex getKnnIndex() {
            return knnIndex;
        }

        private WatcherHandle<FileWatcher> getFileWatcherHandle() {
            return fileWatcherHandle;
        }
    }

    private static FileChangesListener KNN_INDEX_FILE_DELETED_LISTENER = new FileChangesListener() {
        @Override
        public void onFileDeleted(Path indexFilePath) {
            logger.debug("[KNN] Invalidated because file {} is deleted", indexFilePath.toString());
            getInstance().cache.invalidate(indexFilePath.toString());
        }
    };
}
