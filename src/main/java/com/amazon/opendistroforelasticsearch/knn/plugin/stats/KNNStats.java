package com.amazon.opendistroforelasticsearch.knn.plugin.stats;

import java.util.HashMap;
import java.util.Map;

/**
 * Class represents all stats the plugin keeps track of
 */
public class KNNStats {

    private Map<String, KNNStat<?>> knnStats;

    /**
     * Constructor
     *
     * @param knnStats Map that maps name of stat to KNNStat object
     */
    public KNNStats(Map<String, KNNStat<?>> knnStats) {
        this.knnStats = knnStats;
    }

    /**
     * Get the stats
     *
     * @return all of the stats
     */
    public Map<String, KNNStat<?>> getStats() {
        return knnStats;
    }

    /**
     * Get individual stat by stat name
     *
     * @param key Name of stat
     * @return ADStat
     * @throws IllegalArgumentException thrown on illegal statName
     */
    public KNNStat<?> getStat(String key) throws IllegalArgumentException {
        if (!knnStats.keySet().contains(key)) {
            throw new IllegalArgumentException("Stat=\"" + key + "\" does not exist");
        }
        return knnStats.get(key);
    }

    /**
     * Get a map of the stats that are kept at the node level
     *
     * @return Map of stats kept at the node level
     */
    public Map<String, KNNStat<?>> getNodeStats() {
        return getClusterOrNodeStats(false);
    }

    /**
     * Get a map of the stats that are kept at the cluster level
     *
     * @return Map of stats kept at the cluster level
     */
    public Map<String, KNNStat<?>> getClusterStats() {
        return getClusterOrNodeStats(true);
    }

    private Map<String, KNNStat<?>> getClusterOrNodeStats(Boolean getClusterStats) {
        Map<String, KNNStat<?>> statsMap = new HashMap<>();

        for (Map.Entry<String, KNNStat<?>> entry : knnStats.entrySet()) {
            if (entry.getValue().isClusterLevel() == getClusterStats) {
                statsMap.put(entry.getKey(), entry.getValue());
            }
        }
        return statsMap;
    }
}