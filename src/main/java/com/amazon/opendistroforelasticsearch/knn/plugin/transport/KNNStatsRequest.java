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

package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * KNNStatsRequest gets node (cluster) level Stats for KNN
 * By default, all parameters will be true
 */
public class KNNStatsRequest extends BaseNodesRequest<KNNStatsRequest> {

    /**
     * Key indicating all stats should be retrieved
     */
    public static final String ALL_STATS_KEY = "_all";

    private Set<String> validStats;
    private Set<String> statsToBeRetrieved;

    /**
     * Empty constructor needed for KNNStatsTransportAction
     */
    public KNNStatsRequest() {
        super((String[]) null);
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public KNNStatsRequest(StreamInput in) throws IOException {
        super(in);
        validStats = in.readSet(StreamInput::readString);
        statsToBeRetrieved = in.readSet(StreamInput::readString);
    }

    /**
     * Constructor
     *
     * @param validStats set of stat names that are valid for KNN plugin
     * @param nodeIds NodeIDs from which to retrieve stats
     */
    public KNNStatsRequest(Set<String> validStats, String... nodeIds) {
        super(nodeIds);
        this.validStats = validStats;
        statsToBeRetrieved = new HashSet<>();
    }

    /**
     * Add all stats to be retrieved
     */
    public void all() {
        statsToBeRetrieved.addAll(validStats);
    }

    /**
     * Remove all stats from retrieval set
     */
    public void clear() {
        statsToBeRetrieved.clear();
    }

    /**
     * Sets a stats retrieval status to true if it is a valid stat
     * @param stat stat name
     * @return true if the stats's retrieval status is successfully update; false otherwise
     */
    public boolean addStat(String stat) {
        if (validStats.contains(stat)) {
            statsToBeRetrieved.add(stat);
            return true;
        }
        return false;
    }

    /**
     * Get the set that tracks which stats should be retrieved
     *
     * @return the set that contains the stat names marked for retrieval
     */
    public Set<String> getStatsToBeRetrieved() {
        return statsToBeRetrieved;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(validStats);
        out.writeStringCollection(statsToBeRetrieved);
    }
}