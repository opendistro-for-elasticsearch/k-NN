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

package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.List;

/**
 * Response returned for k-NN Warmup. Returns total number of shards Warmup was performed on, as well as
 * the number of shards that succeeded and the number of shards that failed.
 */
public class KNNWarmupResponse extends BroadcastResponse implements ToXContentObject {

    public KNNWarmupResponse() {}

    public KNNWarmupResponse(StreamInput in) throws IOException {
        super(in);
    }

    public KNNWarmupResponse(int totalShards, int successfulShards, int failedShards,
                             List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }
}
