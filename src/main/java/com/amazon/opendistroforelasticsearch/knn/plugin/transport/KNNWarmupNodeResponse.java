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

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class KNNWarmupNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    int graphCount;
    int failureCount;

    public KNNWarmupNodeResponse(StreamInput in) throws IOException {
        super(in);
        graphCount = in.readInt();
        failureCount = in.readInt();
    }

    /**
     * Constructor
     *
     * @param node node
     */
    public KNNWarmupNodeResponse(DiscoveryNode node, int graphCount, int failureCount) {
        super(node);
        this.graphCount = graphCount;
        this.failureCount = failureCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(graphCount);
        out.writeInt(failureCount);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("graph_count", graphCount);
        builder.field("failure_count", failureCount);
        return builder;
    }
}
