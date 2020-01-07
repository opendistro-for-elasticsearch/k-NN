package com.amazon.opendistroforelasticsearch.knn.plugin.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum contains names of the stats
 */
public enum StatNames {
    HIT_COUNT("hit_count"),
    MISS_COUNT("miss_count"),
    LOAD_SUCCESS_COUNT("load_success_count"),
    LOAD_EXCEPTION_COUNT("load_exception_count"),
    TOTAL_LOAD_TIME("total_load_time"),
    EVICTION_COUNT("eviction_count"),
    GRAPH_MEMORY_USAGE("graph_memory_usage"),
    CACHE_CAPACITY_REACHED("cache_capacity_reached"),
    CIRCUIT_BREAKER_TRIGGERED("circuit_breaker_triggered");

    private String name;

    StatNames(String name) { this.name = name; }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() { return name; }

    /**
     * Get all stat names
     *
     * @return set of all stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}