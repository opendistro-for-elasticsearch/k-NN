package com.amazon.opendistroforelasticsearch.knn.plugin.transport;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * KNNStatsAction class
 */
public class KNNStatsAction extends Action<KNNStatsResponse> {

    public static final KNNStatsAction INSTANCE = new KNNStatsAction();
    public static final String NAME = "cluster:admin/knn_stats_action";

    /**
     * Constructor
     */
    private KNNStatsAction() {
        super(NAME);
    }

    @Override
    public KNNStatsResponse newResponse() {
        throw new UnsupportedOperationException("Usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<KNNStatsResponse> getResponseReader() {
        return KNNStatsResponse::new;
    }
}