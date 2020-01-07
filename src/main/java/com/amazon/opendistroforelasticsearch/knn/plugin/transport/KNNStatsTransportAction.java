package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  KNNStatsTransportAction contains the logic to extract the stats from the nodes
 */
public class KNNStatsTransportAction extends TransportNodesAction<KNNStatsRequest, KNNStatsResponse,
        KNNStatsNodeRequest, KNNStatsNodeResponse> {

    private KNNStats knnStats;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param knnStats KNNStats object
     */
    @Inject
    public KNNStatsTransportAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            KNNStats knnStats
            ) {
        super(KNNStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, KNNStatsRequest::new,
                KNNStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, KNNStatsNodeResponse.class);
        this.knnStats = knnStats;
    }

    @Override
    protected KNNStatsResponse newResponse(KNNStatsRequest request, List<KNNStatsNodeResponse> responses,
                                           List<FailedNodeException> failures) {

        Map<String, Object> clusterStats = new HashMap<>();
        Set<String> statsToBeRetrieved = request.getStatsToBeRetrieved();

        for (String statName : knnStats.getClusterStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                clusterStats.put(statName, knnStats.getStats().get(statName).getValue());
            }
        }

        return new KNNStatsResponse(
                clusterService.getClusterName(),
                responses,
                failures,
                clusterStats
        );
    }

    @Override
    protected KNNStatsNodeRequest newNodeRequest(String nodeId, KNNStatsRequest request) {
        return new KNNStatsNodeRequest(nodeId, request);
    }

    @Override
    protected KNNStatsNodeResponse newNodeResponse() {
        return new KNNStatsNodeResponse();
    }

    @Override
    protected KNNStatsNodeResponse nodeOperation(KNNStatsNodeRequest request) {
        return createKNNStatsNodeResponse(request.getKNNStatsRequest());
    }

    private KNNStatsNodeResponse createKNNStatsNodeResponse(KNNStatsRequest knnStatsRequest) {
        Map<String, Object> statValues = new HashMap<>();
        Set<String> statsToBeRetrieved = knnStatsRequest.getStatsToBeRetrieved();

        for (String statName : knnStats.getNodeStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                statValues.put(statName, knnStats.getStats().get(statName).getValue());
            }
        }

        return new KNNStatsNodeResponse(clusterService.localNode(), statValues);
    }
}
