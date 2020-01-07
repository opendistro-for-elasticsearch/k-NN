package com.amazon.opendistroforelasticsearch.knn.plugin.stats;

import java.util.function.Supplier;

/**
 * Class represents a stat the plugin keeps track of
 */
public class KNNStat<T> {
    private Boolean clusterLevel;
    private Supplier<T> supplier;

    /**
     * Constructor
     *
     * @param clusterLevel the scope of the stat
     * @param supplier supplier that returns the stat's value
     */
    public KNNStat(Boolean clusterLevel, Supplier<T> supplier) {
        this.clusterLevel = clusterLevel;
        this.supplier = supplier;
    }

    /**
     * Determines whether the stat is kept at the cluster level or the node level
     *
     * @return boolean that is true if the stat is clusterLevel; false otherwise
     */
    public Boolean isClusterLevel() { return clusterLevel; }

    /**
     * Get the value of the statistic
     *
     * @return value of the stat
     */
    public T getValue() {
        return supplier.get();
    }
}