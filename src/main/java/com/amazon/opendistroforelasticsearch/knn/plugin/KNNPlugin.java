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

package com.amazon.opendistroforelasticsearch.knn.plugin;

import com.amazon.opendistroforelasticsearch.knn.index.KNNCircuitBreaker;
import com.amazon.opendistroforelasticsearch.knn.index.KNNIndexCache;
import com.amazon.opendistroforelasticsearch.knn.index.KNNQueryBuilder;
import com.amazon.opendistroforelasticsearch.knn.index.KNNSettings;
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;

import com.amazon.opendistroforelasticsearch.knn.plugin.rest.RestKNNStatsHandler;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStat;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStats;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.StatNames;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNCacheSupplier;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNCircuitBreakerSupplier;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.suppliers.KNNInnerCacheStatsSupplier;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsTransportAction;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Entry point for the KNN plugin where we define mapper for knn_vector type
 * and new query clause "knn"
 *
 *
 * Example Mapping for knn_vector type
 * "settings" : {
 *    "index": {
 *       "knn": true
 *     }
 *   },
 * "mappings": {
 *   "properties": {
 *     "my_vector": {
 *       "type": "knn_vector",
 *       "dimension": 4
 *     }
 *   }
 * }
 *
 * Example Query
 *
 *   "knn": {
 *    "my_vector": {
 *      "vector": [3, 4],
 *      "k": 3
 *    }
 *   }
 *
 */
public class KNNPlugin extends Plugin implements MapperPlugin, SearchPlugin, ActionPlugin, EnginePlugin {

    public static final String KNN_BASE_URI = "/_opendistro/_knn";

    private KNNStats knnStats;

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(KNNVectorFieldMapper.CONTENT_TYPE, new KNNVectorFieldMapper.TypeParser());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(KNNQueryBuilder.NAME, KNNQueryBuilder::new, KNNQueryBuilder::fromXContent));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        KNNIndexCache.setResourceWatcherService(resourceWatcherService);
        KNNSettings.state().initialize(client, clusterService);
        KNNCircuitBreaker.getInstance().initialize(threadPool, clusterService, client);

        Map<String, KNNStat<?>> stats = ImmutableMap.<String, KNNStat<?>>builder()
                .put(StatNames.HIT_COUNT.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::hitCount)))
                .put(StatNames.MISS_COUNT.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::missCount)))
                .put(StatNames.LOAD_SUCCESS_COUNT.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::loadSuccessCount)))
                .put(StatNames.LOAD_EXCEPTION_COUNT.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::loadExceptionCount)))
                .put(StatNames.TOTAL_LOAD_TIME.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::totalLoadTime)))
                .put(StatNames.EVICTION_COUNT.getName(), new KNNStat<>(false,
                        new KNNInnerCacheStatsSupplier(CacheStats::evictionCount)))
                .put(StatNames.GRAPH_MEMORY_USAGE.getName(), new KNNStat<>(false,
                        new KNNCacheSupplier<>(KNNIndexCache::getWeightInKilobytes)))
                .put(StatNames.CACHE_CAPACITY_REACHED.getName(), new KNNStat<>(false,
                        new KNNCacheSupplier<>(KNNIndexCache::isCacheCapacityReached)))
                .put(StatNames.CIRCUIT_BREAKER_TRIGGERED.getName(), new KNNStat<>(true,
                        new KNNCircuitBreakerSupplier())).build();

        knnStats = new KNNStats(stats);

        return ImmutableList.of(knnStats);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return KNNSettings.state().getSettings();
    }

    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {

        RestKNNStatsHandler restKNNStatsHandler = new RestKNNStatsHandler(settings, restController, knnStats);

        return Arrays.asList(restKNNStatsHandler);
    }

    /**
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(KNNStatsAction.INSTANCE, KNNStatsTransportAction.class)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(KNNSettings.IS_KNN_INDEX_SETTING)) {
            return Optional.of(new KNNEngineFactory());
        }
        return Optional.empty();
    }
}
