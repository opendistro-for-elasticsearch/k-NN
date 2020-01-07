package com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers;
import com.google.common.cache.CacheStats;
import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Supplier for stats of the cache that the KNNCache uses
 */
public class KNNInnerCacheStatsSupplier implements Supplier<Long> {
    Function<CacheStats, Long> getter;

    /**
     * Constructor
     *
     * @param getter CacheStats method to supply a value
     */
    public KNNInnerCacheStatsSupplier(Function<CacheStats, Long> getter) {
        this.getter = getter;
    }

    @Override
    public Long get() {
        return getter.apply(KNNIndexCache.getInstance().cache.stats());
    }
}