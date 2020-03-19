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

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *  KNNStatsNodeRequest represents the request to an individual node
 */
public class KNNStatsNodeRequest extends BaseNodeRequest {
    private KNNStatsRequest request;

    /**
     * Constructor
     */
    public KNNStatsNodeRequest() {
        super();
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public KNNStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new KNNStatsRequest(in);
    }

    /**
     * Constructor
     *
     * @param request KNNStatsRequest
     */
    public KNNStatsNodeRequest(KNNStatsRequest request) {
        this.request = request;
    }

    /**
     * Get KNNStatsRequest
     *
     * @return KNNStatsRequest for this node
     */
    public KNNStatsRequest getKNNStatsRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}