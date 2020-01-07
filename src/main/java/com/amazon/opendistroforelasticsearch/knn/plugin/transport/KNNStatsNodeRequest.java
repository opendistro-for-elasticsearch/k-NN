package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *  KNNStatsNodeRequest represents the request to an individual node
 */
class KNNStatsNodeRequest extends BaseNodeRequest {
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
     * @param nodeId Node ID
     * @param request KNNStatsRequest
     */
    public KNNStatsNodeRequest(String nodeId, KNNStatsRequest request) {
        super(nodeId);
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = new KNNStatsRequest();
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}