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

package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_SPACE_TYPE;

import static org.elasticsearch.Version.V_7_1_0;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNVectorFieldMapperTests extends KNNTestCase {

    public void testBuildKNNIndexSettings_emptySettings_checkDefaultsSet() {
        String indexName = "test-index";
        String fieldName = "test-fieldname";

        Mapper.TypeParser.ParserContext parserContext = mock(Mapper.TypeParser.ParserContext.class);
        MapperService mapperService = mock(MapperService.class);
        IndexSettings indexSettings = new IndexSettings(
                IndexMetaData.builder(indexName).settings(settings(V_7_1_0))
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .version(7)
                        .mappingVersion(0)
                        .settingsVersion(0)
                        .aliasesVersion(0)
                        .creationDate(0)
                        .build(),
                settings(V_7_1_0).build());
        when(parserContext.mapperService()).thenReturn(mapperService);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        KNNVectorFieldMapper.Builder builder = new KNNVectorFieldMapper.Builder(fieldName);

        KNNVectorFieldMapper.TypeParser typeParser = new KNNVectorFieldMapper.TypeParser();
        typeParser.buildKNNIndexSettings(builder, parserContext);

        assertEquals(KNNVectorFieldMapper.Defaults.FIELD_TYPE.getAttributes().get(KNNConstants.SPACE_TYPE),
                INDEX_KNN_DEFAULT_SPACE_TYPE);

        assertEquals(KNNVectorFieldMapper.Defaults.FIELD_TYPE.getAttributes().get(KNNConstants.HNSW_ALGO_M),
                String.valueOf(INDEX_KNN_DEFAULT_ALGO_PARAM_M));

        assertEquals(KNNVectorFieldMapper.Defaults.FIELD_TYPE.getAttributes().get(
                KNNConstants.HNSW_ALGO_EF_CONSTRUCTION), String.valueOf(
                        INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION));
    }
}