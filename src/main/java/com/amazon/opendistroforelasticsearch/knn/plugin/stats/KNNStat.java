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