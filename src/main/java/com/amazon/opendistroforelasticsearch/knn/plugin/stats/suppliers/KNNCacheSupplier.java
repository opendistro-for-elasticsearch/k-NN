package com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers;

import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Supplier for stats of KNNIndexCache
 */
public class KNNCacheSupplier<T> implements Supplier<T> {
    private Function<KNNIndexCache, T> getter;

    /**
     * Constructor
     *
     * @param getter KNNIndexCache Method to supply a value
     */
    public KNNCacheSupplier(Function<KNNIndexCache, T> getter) {
        this.getter = getter;
    }

    @Override
    public T get() {
        return getter.apply(KNNIndexCache.getInstance());
    }
}