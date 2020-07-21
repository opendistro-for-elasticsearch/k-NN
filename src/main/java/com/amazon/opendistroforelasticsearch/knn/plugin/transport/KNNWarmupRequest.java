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

import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * k-NN Warmup Request. This request contains a list of indices for which warmup should be performed.
 */
public class KNNWarmupRequest extends BroadcastRequest<KNNWarmupRequest> {

    public KNNWarmupRequest(StreamInput in) throws IOException {
        super(in);
    }

    public KNNWarmupRequest(String... indices) {
        super(indices);
    }
}
