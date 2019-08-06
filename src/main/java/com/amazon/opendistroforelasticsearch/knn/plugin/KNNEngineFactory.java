package com.amazon.opendistroforelasticsearch.knn.plugin;

import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;

/**
 * EngineFactory to inject the KNNCodecService to help segments write using the KNNCodec.
 */
class KNNEngineFactory implements EngineFactory {

    private static CodecService codecService = new KNNCodecService();

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        EngineConfig engineConfig = new EngineConfig(config.getShardId(), config.getAllocationId(), config.getThreadPool(),
                config.getIndexSettings(), config.getWarmer(), config.getStore(), config.getMergePolicy(), config.getAnalyzer(),
                config.getSimilarity(), codecService, config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(),
                config.getTranslogConfig(), config.getFlushMergesAfter(), config.getExternalRefreshListener(), config.getInternalRefreshListener(),
                config.getIndexSort(), config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(), config.getPrimaryTermSupplier(), config.getTombstoneDocSupplier());
        return new InternalEngine(engineConfig);
    }
}
