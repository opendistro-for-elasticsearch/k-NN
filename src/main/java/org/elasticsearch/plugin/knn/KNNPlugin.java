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

package org.elasticsearch.plugin.knn;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.knn.KNNIndexCache;
import org.elasticsearch.index.knn.KNNIndexFileListener;
import org.elasticsearch.index.knn.KNNQueryBuilder;
import org.elasticsearch.index.knn.KNNVectorFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Entry point for the KNN plugin where we define mapper for knn_vector type
 * and new query clause "knn"
 *
 *
 * Example Mapping for knn_vector type
 *
 * "mappings": {
 *   "properties": {
 *     "my_vector": {
 *       "type": "knn_vector"
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
public class KNNPlugin extends Plugin implements MapperPlugin, SearchPlugin, ActionPlugin {

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
        KNNIndexFileListener knnIndexFileListener = new KNNIndexFileListener(resourceWatcherService);
        KNNIndexCache.setKnnIndexFileListener(knnIndexFileListener);
        return Collections.singletonList(knnIndexFileListener);
    }

}
