/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import com.amazon.opendistroforelasticsearch.knn.index.v1736.KNNIndex;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.StatNames;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.WatcherHandle;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.getCircuitBreakerLimit;

/**
 * KNNIndex level caching with weight based, time based evictions. This caching helps us
 * to manage the hnsw graphs in the memory and garbage collect them after specified timeout
 * or when weightCircuitBreaker is hit.
 */
public class KNNIndexCache implements Closeable {
    public static String GRAPH_COUNT = "graph_count";

    private static Logger logger = LogManager.getLogger(KNNIndexCache.class);

    private static KNNIndexCache INSTANCE;

    private Cache<String, KNNIndexCacheEntry> cache;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ThreadPool threadPool;
    private AtomicBoolean cacheCapacityReached;
    private ResourceWatcherService resourceWatcherService;
    private ClusterService clusterService;
    private NodeEnvironment nodeEnvironment;

    private KNNIndexCache() {
        initCache();
    }

    public static void setResourceWatcherService(final ResourceWatcherService resourceWatcherService) {
        getInstance().resourceWatcherService = resourceWatcherService;
    }

    public static void setClusterService(final ClusterService clusterService) {
        getInstance().clusterService = clusterService;
    }

    public static void setNodeEnvironment(final NodeEnvironment nodeEnvironment) {
        getInstance().nodeEnvironment = nodeEnvironment;
    }

    public static void setThreadPool(final ThreadPool threadPool) {
        getInstance().threadPool = threadPool;
    }

    public void close() {
        executor.shutdown();
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
            cacheBuilder.maximumWeight(getCircuitBreakerLimit().getKb()).weigher((k, v) -> (int)v.getKnnIndex().getIndexSize());
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

        String esIndexName = removalNotification.getValue().getEsIndexName();
        String indexPathUrl = removalNotification.getValue().getIndexPathUrl();

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
     * @param indexName index name
     * @return KNNIndex holding the heap pointer of the loaded graph
     */
    public KNNIndex getIndex(String key, final String indexName) {
        try {
            final KNNIndexCacheEntry knnIndexCacheEntry = cache.get(key, () -> loadIndex(key, indexName));
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
     * Get the stats of all of the Elasticsearch indices currently loaded into the cache
     *
     * @return Map containing all of the Elasticsearch indices in the cache and their stats
     */
    public Map<String, Map<String, Object>> getIndicesCacheStats() {
        Map<String, Map<String, Object>> statValues = new HashMap<>();
        String indexName;
        for (Map.Entry<String, KNNIndexCacheEntry> index : cache.asMap().entrySet()) {
            indexName = index.getValue().getEsIndexName();
            statValues.putIfAbsent(indexName, new HashMap<>());
            statValues.get(indexName).put(GRAPH_COUNT, ((Integer) statValues.get(indexName)
                    .getOrDefault(GRAPH_COUNT, 0)) + 1);
            statValues.get(indexName).putIfAbsent(StatNames.GRAPH_MEMORY_USAGE.getName(),
                    getWeightInKilobytes(indexName));
            statValues.get(indexName).putIfAbsent(StatNames.GRAPH_MEMORY_USAGE_PERCENTAGE.getName(),
                    getWeightAsPercentage(indexName));
        }
        
        return statValues;
    }

    protected Set<String> getGraphNamesForIndex(String indexName) {
        return cache.asMap().values().stream()
                .filter(knnIndexCacheEntry -> indexName.equals(knnIndexCacheEntry.getEsIndexName()))
                .map(KNNIndexCacheEntry::getIndexPathUrl)
                .collect(Collectors.toSet());
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
     * Returns the current weight of an index in the cache in KiloBytes
     *
     * @param indexName Name if index to get the weight for
     * @return Weight of the index in the cache in kilobytes
     */
    public Long getWeightInKilobytes(final String indexName) {
        return cache.asMap().values().stream()
                .filter(knnIndexCacheEntry -> indexName.equals(knnIndexCacheEntry.getEsIndexName()))
                .map(KNNIndexCacheEntry::getKnnIndex).mapToLong(KNNIndex::getIndexSize).sum();
    }

    /**
     * Returns how full the cache is as a percentage of the total cache capacity
     *
     * @return Percentage of the cache full
     */
    public Float getWeightAsPercentage() {
        return 100 * getWeightInKilobytes() / (float) getCircuitBreakerLimit().getKb();
    }

    /**
     * Returns the how much space an index is taking up in the cache is as a percentage of the total cache capacity
     *
     * @return Percentage of the cache full
     */
    public Float getWeightAsPercentage(final String indexName) {
        return 100 * getWeightInKilobytes(indexName) / (float) getCircuitBreakerLimit().getKb();
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
     * Evict a graph in the cache manually
     *
     * @param indexFilePath path to segment file. Also, key in cache
     */
    public void evictGraphFromCache(String indexFilePath) {
        logger.info("[KNN] " + indexFilePath  + " invalidated explicitly");
        cache.invalidate(indexFilePath);
    }

    /**
     * Evict all graphs in the cache manually
     */
    public void evictAllGraphsFromCache() {
        logger.info("[KNN] All entries in cache invalidated explicitly");
        cache.invalidateAll();
    }

    /**
     * Loads k-NN Lucene index to memory. Registers the location of the serialized graph with ResourceWatcher.
     *
     * @param segmentPath path for serialized k-NN segment
     * @param indexName index name
     * @return KNNIndex holding the heap pointer of the loaded graph
     * @throws Exception Exception could occur when registering the index path
     * to Resource watcher or if the JNI call throws
     */
    public KNNIndexCacheEntry loadIndex(String segmentPath, String indexName) throws Exception {
        if(Strings.isNullOrEmpty(segmentPath))
            throw new IllegalStateException("indexPath is null while performing load index");
        logger.info("[KNN] Loading index: {}", segmentPath);
        Path indexPath = Paths.get(segmentPath);
        FileWatcher fileWatcher = new FileWatcher(indexPath);
        fileWatcher.addListener(KNN_INDEX_FILE_DELETED_LISTENER);

        // Calling init() on the FileWatcher will bootstrap initial state that indicates whether or not the file
        // is present. If it is not present at time of init(), then KNNIndex.loadIndex will fail and we won't cache
        // the entry
        fileWatcher.init();

        final KNNIndex knnIndex = KNNIndex.loadIndex(segmentPath, getQueryParams(indexName), KNNSettings.getSpaceType(indexName));

        // TODO verify that this is safe - ideally we'd explicitly ensure that the FileWatcher is only checked
        // after the guava cache has finished loading the key to avoid a race condition where the watcher
        // causes us to invalidate an entry before the key has been fully loaded.
        final WatcherHandle<FileWatcher> watcherHandle = resourceWatcherService.add(fileWatcher);

        return new KNNIndexCacheEntry(knnIndex, segmentPath, indexName, watcherHandle);
    }


    public boolean loadIndex(IndexShard indexShard) {
        try {
            for (String hnswPath : getHNSWPaths(indexShard.shardPath())) {
                threadPool.executor(ThreadPool.Names.GENERIC).submit(
                        () -> cache.get(hnswPath, () -> loadIndex(hnswPath, indexShard.shardId().getIndexName()))
                );
            }
        } catch (IOException ex) {
            logger.error("[KNN] Unable to load shard={} into graph: {}", indexShard.shardId(), ex);
            return false;
        }

        return true;
    }

//    /**
//     * Loads all of the segments for the Elasticsearch index into the cache
//     *
//     * I think functionality of getting segment information should be encapsulated in the KNNIndex class.
//     *
//     * @param indexName Name of index
//     * @throws IOException thrown if index is invalid
//     */
//    public void loadIndex(String indexName) throws IOException {
//        Index index = this.clusterService.state().getMetaData().getIndices().get(indexName).getIndex();
//        Set<ShardId> shardIds = nodeEnvironment.findAllShardIds(index);
//
//        for (ShardId shardId : shardIds) {
//            threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
//                try {
//                    for (String hnswPath : getIndexPaths(shardId)) {
//                        cache.get(hnswPath, () -> loadIndex(hnswPath, indexName));
//                    }
//                } catch (Exception ex) {
//                    logger.error("[KNN] Unable to load shard={} into graph: {}", shardId, ex);
//                }
//            });
//        }
//    }

    private List<String> getHNSWPaths(ShardPath shardPath) throws IOException {
        IndexReader indexReader = getIndexReader(shardPath);
        List<String> hnswFiles = new ArrayList<>();
        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(leafReaderContext.reader());
            hnswFiles.addAll(reader.getSegmentInfo().files().stream()
                    .filter(fileName -> fileName.endsWith(getHnswFileExtension(reader)))
                    .map(fileName -> shardPath.resolveIndex().resolve(fileName).toString())
                    .collect(Collectors.toList()));
        }

        return hnswFiles;
    }

    private List<String> getIndexPaths(ShardId shardId, String fieldName) throws IOException {
        ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnvironment, shardId, null);
        IndexReader indexReader = getIndexReader(shardPath);
        List<String> hnswFiles = new ArrayList<>();
        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(leafReaderContext.reader());
            hnswFiles.addAll(reader.getSegmentInfo().files().stream()
                    .filter(fileName -> fileName.endsWith(getHnswFileExtension(reader)))
                    .filter(fileName -> fileName.contains(fieldName))
                    .map(fileName -> shardPath.resolveIndex().resolve(fileName).toString())
                    .collect(Collectors.toList()));
        }

        return hnswFiles;
    }

    private IndexReader getIndexReader(ShardPath shardPath) throws IOException {
        Directory directory = FSDirectory.open(shardPath.resolveIndex());
        return DirectoryReader.open(directory);
    }

    private String getHnswFileExtension(SegmentReader reader) {
        return reader.getSegmentInfo().info.getUseCompoundFile() ? KNNCodecUtil.HNSW_COMPOUND_EXTENSION :
                KNNCodecUtil.HNSW_EXTENSION;
    }

    /**
     * KNNIndexCacheEntry is the value type for entries in the cache held by {@link KNNIndexCache}.
     * It holds a reference to both the KNNIndex and the WatcherHandle so that each can be cleaned up
     * upon expiration of the cache.
     */
    private static class KNNIndexCacheEntry {
        private final KNNIndex knnIndex;
        private final String indexPathUrl;
        private final String esIndexName;
        private final WatcherHandle<FileWatcher> fileWatcherHandle;

        private KNNIndexCacheEntry(final KNNIndex knnIndex, final String indexPathUrl, final String esIndexName,
                                   final WatcherHandle<FileWatcher> fileWatcherHandle) {
            this.knnIndex = knnIndex;
            this.indexPathUrl = indexPathUrl;
            this.esIndexName = esIndexName;
            this.fileWatcherHandle = fileWatcherHandle;
        }

        private KNNIndex getKnnIndex() {
            return knnIndex;
        }

        private String getIndexPathUrl() {
            return indexPathUrl;
        }

        private String getEsIndexName() {
            return esIndexName;
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

    private String[] getQueryParams(String indexName) {
        return new String[] {"efSearch=" + KNNSettings.getEfSearchParam(indexName)};
    }
}
