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
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashSet;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_PQ;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.KNN_ENGINE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.KNN_METHOD;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_IVF;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_CODE_SIZE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NCENTROIDS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.elasticsearch.Version.CURRENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNVectorFieldMapperTests extends KNNTestCase {
    /**
     * Test that we can successfully create builder and get the relevant values. Note that parse needs to be called
     * in order to set the relevant parameters. Without calling parse, only the defaults will be set
     */
    public void testBuilder_build() {
        KNNVectorFieldMapper.Builder builder = new KNNVectorFieldMapper.Builder("test-field-name-1");

        // For default settings, everything in KNNVectorFieldMapper should be default after calling build
        Settings settings = Settings.builder()
                .put(settings(CURRENT).build())
                .build();
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        KNNVectorFieldMapper knnVectorFieldMapper = builder.build(builderContext);

        assertNotNull(knnVectorFieldMapper);
        assertEquals(KNNEngine.DEFAULT.getName(), knnVectorFieldMapper.knnEngine);
        assertEquals(SpaceType.DEFAULT.getValue(), knnVectorFieldMapper.spaceType);
        assertEquals(KNNEngine.DEFAULT.getMethod(METHOD_HNSW).getMethodComponent().getParameters()
                .get(METHOD_PARAMETER_M).getDefaultValue().toString(), knnVectorFieldMapper.m);
        assertEquals(KNNEngine.DEFAULT.getMethod(METHOD_HNSW).getMethodComponent().getParameters()
                .get(METHOD_PARAMETER_EF_CONSTRUCTION).getDefaultValue().toString(),
                knnVectorFieldMapper.efConstruction);


        // When passing spaceType, efConstruction and m settings, these should be set. This only applies to nmslib.
        // We do not allow this way of passing parameters for faiss. By default, the nmslib engine is used, so we do
        // not have to configure it.
        String spaceType = SpaceType.COSINESIMIL.getValue();
        int m = 111;
        int efConstruction = 192;

        builder = new KNNVectorFieldMapper.Builder("test-field-name-2");

        settings = Settings.builder()
                .put(settings(CURRENT).build())
                .put(KNNSettings.KNN_SPACE_TYPE, spaceType)
                .put(KNNSettings.KNN_ALGO_PARAM_M, m)
                .put(KNNSettings.KNN_ALGO_PARAM_EF_CONSTRUCTION, efConstruction)
                .build();
        builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        knnVectorFieldMapper = builder.build(builderContext);

        assertEquals(spaceType, knnVectorFieldMapper.spaceType);
        assertEquals(Integer.toString(m), knnVectorFieldMapper.m);
        assertEquals(Integer.toString(efConstruction), knnVectorFieldMapper.efConstruction);

        // Test that method settings get precedence over mapping parameters
        int m1 = 1000;
        int efConstruction1 = 12;
        SpaceType spaceType1 = SpaceType.L1;
        builder = new KNNVectorFieldMapper.Builder("test-field-name-3");
        builder.knnMethodContext.setValue(new KNNMethodContext(KNNEngine.NMSLIB, spaceType1,
                new MethodComponentContext(METHOD_HNSW,
                        ImmutableMap.of(METHOD_PARAMETER_M, m1,
                                METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction1)),
                null, null, 1, 1));

        settings = Settings.builder()
                .put(settings(CURRENT).build())
                .put(KNNSettings.KNN_SPACE_TYPE, spaceType)
                .put(KNNSettings.KNN_ALGO_PARAM_M, m)
                .put(KNNSettings.KNN_ALGO_PARAM_EF_CONSTRUCTION, efConstruction)
                .build();
        builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        knnVectorFieldMapper = builder.build(builderContext);

        assertEquals(spaceType, knnVectorFieldMapper.spaceType);
        assertEquals(Integer.toString(m), knnVectorFieldMapper.m);
        assertEquals(Integer.toString(efConstruction), knnVectorFieldMapper.efConstruction);

        // When settings are empty, mapping parameters are used
        builder = new KNNVectorFieldMapper.Builder("test-field-name-4");
        builder.knnMethodContext.setValue(new KNNMethodContext(KNNEngine.NMSLIB, spaceType1,
                new MethodComponentContext(METHOD_HNSW,
                        ImmutableMap.of(METHOD_PARAMETER_M, m1,
                                METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction1)),
                null, null, 1, 1));

        settings = Settings.builder()
                .put(settings(CURRENT).build())
                .build();
        builderContext = new Mapper.BuilderContext(settings, new ContentPath());
        knnVectorFieldMapper = builder.build(builderContext);

        assertEquals(spaceType1.getValue(), knnVectorFieldMapper.spaceType);
        assertEquals(Integer.toString(m1), knnVectorFieldMapper.m);
        assertEquals(Integer.toString(efConstruction1), knnVectorFieldMapper.efConstruction);
    }

    /**
     * Test that the builder correctly returns the parameters on call to getParameters
     */
    public void testBuilder_getParameters() {
        String fieldName = "test-field-name";
        KNNVectorFieldMapper.Builder builder = new KNNVectorFieldMapper.Builder(fieldName);
        assertEquals(5, builder.getParameters().size());
    }

    /**
     * Check that type parsing works for nmslib methods
     */
    public void testTypeParser_nmslib() throws IOException {
        String fieldName = "test-field-name";
        String indexName = "test-index-name";

        Settings settings = Settings.builder()
                .put(settings(CURRENT).build())
                .build();

        KNNVectorFieldMapper.TypeParser typeParser = new KNNVectorFieldMapper.TypeParser();

        int dimension = 133;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .endObject();

        KNNVectorFieldMapper.Builder builder = (KNNVectorFieldMapper.Builder) typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder), buildParserContext(indexName, settings));

        assertEquals(dimension, builder.dimension.get().intValue());
        assertEquals(KNNEngine.NMSLIB, builder.knnMethodContext.get().getEngine());
        assertEquals(SpaceType.DEFAULT, builder.knnMethodContext.get().getSpaceType());
        assertEquals(METHOD_HNSW, builder.knnMethodContext.get().getMethodComponent().getName());
        assertEquals(METHOD_HNSW, builder.knnMethodContext.get().getMethodComponent().getName());

        // Now, we need to test a custom parser
        int efConstruction = 321;
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction)
                .endObject()
                .endObject()
                .endObject();

        builder = (KNNVectorFieldMapper.Builder) typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder), buildParserContext(indexName, settings));

        assertEquals(METHOD_HNSW, builder.knnMethodContext.get().getMethodComponent().getName());
        assertEquals(efConstruction, builder.knnMethodContext.get().getMethodComponent().getParameters()
                .get(METHOD_PARAMETER_EF_CONSTRUCTION));

        // Test invalid parameter
        XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field("invalid", "invalid")
                .endObject()
                .endObject()
                .endObject();

        expectThrows(ValidationException.class, () -> typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder2), buildParserContext(indexName, settings)));

        // Test invalid method
        XContentBuilder xContentBuilder3 = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(NAME, "invalid")
                .endObject()
                .endObject();

        expectThrows(ValidationException.class, () -> typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder3), buildParserContext(indexName, settings)));

        // Test missing required parameter: dimension
        XContentBuilder xContentBuilder4 = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector").endObject();

        expectThrows(IllegalArgumentException.class, () -> typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder4), buildParserContext(indexName, settings)));
    }

    /**
     * Check that type parsing works for faiss methods
     */
    public void testTypeParser_faiss() throws IOException {
        String fieldName = "test-field-name";
        String indexName = "test-index-name";

        Settings settings = Settings.builder()
                .put(settings(CURRENT).build())

                .build();

        KNNVectorFieldMapper.TypeParser typeParser = new KNNVectorFieldMapper.TypeParser();

        int dimension = 2048;

        // HNSW index with m and ef construction set
        int m = 322;
        int efConstruction = 999;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction)
                .field(METHOD_PARAMETER_M, m)
                .endObject()
                .endObject()
                .endObject();

        KNNVectorFieldMapper.Builder builder = (KNNVectorFieldMapper.Builder) typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder), buildParserContext(indexName, settings));

        assertEquals(dimension, builder.dimension.get().intValue());
        assertEquals(KNNEngine.FAISS, builder.knnMethodContext.get().getEngine());
        assertEquals(SpaceType.INNER_PRODUCT, builder.knnMethodContext.get().getSpaceType());

        MethodComponentContext methodComponentContext = builder.knnMethodContext.get()
                .getMethodComponent();
        assertEquals(METHOD_HNSW, methodComponentContext.getName());
        assertEquals(m, methodComponentContext.getParameters().get(METHOD_PARAMETER_M));
        assertEquals(efConstruction, methodComponentContext.getParameters().get(METHOD_PARAMETER_EF_CONSTRUCTION));

        // IVF index with nprobes set
        int nprobes = 455;
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NPROBES, nprobes)
                .endObject()
                .endObject()
                .endObject();

        builder = (KNNVectorFieldMapper.Builder) typeParser.parse(fieldName, xContentBuilderToMap(xContentBuilder),
                buildParserContext(indexName, settings));

        methodComponentContext = builder.knnMethodContext.get().getMethodComponent();
        assertEquals(METHOD_IVF, methodComponentContext.getName());
        assertEquals(nprobes, methodComponentContext.getParameters().get(METHOD_PARAMETER_NPROBES));

        // Complex index with encoders and multiple nested components
        /*
             "method": {
                 "engine": faiss,
                 "spaceType": "innerproduct",
                 "training_dataset_size_limit": 1000,
                 "minimum_datapoints": 200,
                 "name": "ivf",
                 "parameters": {
                     "ncentroids": 26500,
                     "nprobes": 128
                 },
                 "coarse_quantizer": {
                     "name": "ivf",
                     "parameters": {
                         "ncentroids": 4,
                         "nprobes": 1
                     },
                     "coarse_quantizer": {
                        "name": "hnsw"
                     },
                     "encoder": {
                         "name": "pq",
                         "parameters": {
                             "code_size": 2
                         }
                     }
                 },
                 "encoder": {
                     "name": "pq",
                     "parameters": {
                         "code_size": 4
                     }
                 }
             }
         */

        int ncentroids1 = 26500;
        int nprobes1 = 128;
        int codeSize1 = 4;

        int ncentroids2 = 4;
        int nprobes2 = 1;
        int codeSize2 = 2;

        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NCENTROIDS, ncentroids1)
                .field(METHOD_PARAMETER_NPROBES, nprobes1)
                .endObject()
                .startObject(COARSE_QUANTIZER)
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NCENTROIDS, ncentroids2)
                .field(METHOD_PARAMETER_NPROBES, nprobes2)
                .endObject()
                .startObject(COARSE_QUANTIZER)
                .field(NAME, METHOD_HNSW)
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, codeSize2)
                .endObject()
                .endObject()
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, codeSize1)
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        builder = (KNNVectorFieldMapper.Builder) typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder), buildParserContext(indexName, settings));

        // First nested check
        MethodComponentContext mainMethodComp1 = builder.knnMethodContext.get().getMethodComponent();
        assertEquals(METHOD_IVF, mainMethodComp1.getName());
        assertEquals(nprobes1, mainMethodComp1.getParameters().get(METHOD_PARAMETER_NPROBES));
        assertEquals(ncentroids1, mainMethodComp1.getParameters().get(METHOD_PARAMETER_NCENTROIDS));

        MethodComponentContext encoderComp1 = builder.knnMethodContext.get().getEncoder();
        assertEquals(ENCODER_PQ, encoderComp1.getName());
        assertEquals(codeSize1, encoderComp1.getParameters().get(METHOD_PARAMETER_CODE_SIZE));

        // Second nested check
        KNNMethodContext knnMethodContext2 = builder.knnMethodContext.get().getCoarseQuantizer();
        MethodComponentContext mainMethodComp2 = knnMethodContext2.getMethodComponent();

        assertEquals(METHOD_IVF, mainMethodComp2.getName());
        assertEquals(nprobes2, mainMethodComp2.getParameters().get(METHOD_PARAMETER_NPROBES));
        assertEquals(ncentroids2, mainMethodComp2.getParameters().get(METHOD_PARAMETER_NCENTROIDS));

        MethodComponentContext encoderComp2 = knnMethodContext2.getEncoder();
        assertEquals(ENCODER_PQ, encoderComp2.getName());
        assertEquals(codeSize2, encoderComp2.getParameters().get(METHOD_PARAMETER_CODE_SIZE));

        // Third nested check
        KNNMethodContext knnMethodContext3 = knnMethodContext2.getCoarseQuantizer();
        MethodComponentContext mainMethodComp3 = knnMethodContext3.getMethodComponent();
        assertEquals(METHOD_HNSW, mainMethodComp3.getName());

        // Test failure on component that does not support coarse quantization
        XContentBuilder xContentBuilder1 = XContentFactory.jsonBuilder().startObject()
                .field("type", "knn_vector")
                .field("dimension", dimension)
                .startObject(KNN_METHOD)
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction)
                .field(METHOD_PARAMETER_M, m)
                .endObject()
                .startObject(COARSE_QUANTIZER)
                .field(NAME, METHOD_IVF)
                .endObject()
                .endObject()
                .endObject();

        expectThrows(ValidationException.class, () -> typeParser.parse(fieldName,
                xContentBuilderToMap(xContentBuilder1), buildParserContext(indexName, settings)));
    }

    public IndexMetadata buildIndexMetaData(String indexName, Settings settings) {
        return IndexMetadata.builder(indexName).settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .version(7)
                .mappingVersion(0)
                .settingsVersion(0)
                .aliasesVersion(0)
                .creationDate(0)
                .build();
    }

    public Mapper.TypeParser.ParserContext buildParserContext(String indexName, Settings settings) {
        IndexSettings indexSettings = new IndexSettings(buildIndexMetaData(indexName, settings), Settings.EMPTY,
                new IndexScopedSettings(Settings.EMPTY, new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS)));
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        // Setup blank
        return new Mapper.TypeParser.ParserContext(null, mapperService,
                type -> new KNNVectorFieldMapper.TypeParser(), CURRENT, null, null, null);

    }
}
