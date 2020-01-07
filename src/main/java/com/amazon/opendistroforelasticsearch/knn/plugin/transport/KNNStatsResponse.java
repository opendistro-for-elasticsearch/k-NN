package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * KNNStatsResponse consists of the aggregated responses from the nodes
 */
public class KNNStatsResponse extends BaseNodesResponse<KNNStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";
    private Map<String, Object> clusterStats;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public KNNStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(KNNStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
        clusterStats = in.readMap();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of KNNStatsNodeResponses
     * @param failures List of failures from nodes
     * @param clusterStats Cluster level stats only obtained from a single node
     */
    public KNNStatsResponse(ClusterName clusterName, List<KNNStatsNodeResponse> nodes, List<FailedNodeException> failures,
                     Map<String, Object> clusterStats) {
        super(clusterName, nodes, failures);
        this.clusterStats = clusterStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(clusterStats);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<KNNStatsNodeResponse> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public List<KNNStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(KNNStatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Return cluster level stats
        for (Map.Entry<String, Object> clusterStat : clusterStats.entrySet()) {
            builder.field(clusterStat.getKey(), clusterStat.getValue());
        }

        // Return node level stats
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (KNNStatsNodeResponse knnStats : getNodes()) {
            node = knnStats.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);
            knnStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}