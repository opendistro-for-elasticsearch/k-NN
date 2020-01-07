package com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers;
import com.amazon.opendistroforelasticsearch.knn.index.KNNSettings;

import java.util.function.Supplier;

/**
 * Supplier for circuit breaker stats
 */
public class KNNCircuitBreakerSupplier implements Supplier<Boolean> {

    /**
     * Constructor
     */
    public KNNCircuitBreakerSupplier() {}

    @Override
    public Boolean get() {
        return KNNSettings.isCircuitBreakerTriggered();
    }
}