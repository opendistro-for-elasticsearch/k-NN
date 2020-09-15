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
import com.amazon.opendistroforelasticsearch.knn.plugin.rest.RestKNNWarmupHandler;
import com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringScriptEngine;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStats;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNStatsTransportAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupAction;
import com.amazon.opendistroforelasticsearch.knn.plugin.transport.KNNWarmupTransportAction;
import com.google.common.collect.ImmutableList;

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
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
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

import static com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNStatsConfig.KNN_STATS;
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
public class KNNPlugin extends Plugin implements MapperPlugin, SearchPlugin, ActionPlugin, EnginePlugin, ScriptPlugin {

    public static final String KNN_BASE_URI = "/_opendistro/_knn";

    private KNNStats knnStats;
    private ClusterService clusterService;

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
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.clusterService = clusterService;
        KNNIndexCache.setResourceWatcherService(resourceWatcherService);
        KNNSettings.state().initialize(client, clusterService);
        KNNCircuitBreaker.getInstance().initialize(threadPool, clusterService, client);
        knnStats = new KNNStats(KNN_STATS);
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
        RestKNNWarmupHandler restKNNWarmupHandler = new RestKNNWarmupHandler(settings, restController, clusterService,
                indexNameExpressionResolver);

        return Arrays.asList(restKNNStatsHandler, restKNNWarmupHandler);
    }

    /**
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(KNNStatsAction.INSTANCE, KNNStatsTransportAction.class),
                new ActionHandler<>(KNNWarmupAction.INSTANCE, KNNWarmupTransportAction.class)
        );
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(KNNSettings.IS_KNN_INDEX_SETTING)) {
            return Optional.of(new KNNEngineFactory());
        }
        return Optional.empty();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        KNNSettings.state().onIndexModule(indexModule);
    }

    /**
     * Sample knn custom script
     *
     * {
     *   "query": {
     *     "script_score": {
     *       "query": {
     *         "match_all": {
     *           "boost": 1
     *         }
     *       },
     *       "script": {
     *         "source": "knn_score",
     *         "lang": "knn",
     *         "params": {
     *           "field": "my_dense_vector",
     *           "vector": [
     *             1,
     *             1
     *           ]
     *         }
     *       }
     *     }
     *   }
     * }
     *
     */
    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new KNNScoringScriptEngine();
    }
}
