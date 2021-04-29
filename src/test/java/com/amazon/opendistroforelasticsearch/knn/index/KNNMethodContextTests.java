/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNLibrary;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_FLAT;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_PQ;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.KNN_ENGINE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_IVF;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_CODE_SIZE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NCENTROIDS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.MINIMUM_DATAPOINTS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.SPACE_TYPE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.TRAINING_DATASET_SIZE_LIMIT;

public class KNNMethodContextTests extends KNNTestCase {
    /**
     * Test method component getter
     */
    public void testGetMethodComponent() {
        MethodComponentContext methodComponent = new MethodComponentContext(
                "test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.DEFAULT, methodComponent,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals(methodComponent, knnMethodContext.getMethodComponent());
    }

    /**
     * Test engine getter
     */
    public void testGetEngine() {
        MethodComponentContext methodComponent = new MethodComponentContext(
                "test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.DEFAULT, methodComponent,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals(KNNEngine.DEFAULT, knnMethodContext.getEngine());
    }

    /**
     * Test spaceType getter
     */
    public void testGetSpaceType() {
        MethodComponentContext methodComponent = new MethodComponentContext(
                "test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, methodComponent,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals(SpaceType.L1, knnMethodContext.getSpaceType());
    }

    /**
     * Test coarse quantizer getter
     */
    public void testGetCoarseQuantizer() {
        MethodComponentContext quantizerMethod = new MethodComponentContext("quantizer-method", Collections.emptyMap());
        KNNMethodContext quantizerContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, quantizerMethod,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        MethodComponentContext methodComponent = new MethodComponentContext(
                "test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, methodComponent,
                quantizerContext, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        assertEquals(quantizerContext, knnMethodContext.getCoarseQuantizer());
    }

    /**
     * Test encoder getter
     */
    public void testGetEncoder() {
        MethodComponentContext encoder = new MethodComponentContext("encoder-method", Collections.emptyMap());

        MethodComponentContext methodComponent = new MethodComponentContext("test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, methodComponent,
                null, encoder, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        assertEquals(encoder, knnMethodContext.getEncoder());
    }

    /**
     * Test training dataset size limit getter
     */
    public void testGetTrainingDatasetSizeLimit() {
        MethodComponentContext methodComponent = new MethodComponentContext("test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, methodComponent,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        assertEquals(KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                knnMethodContext.getTrainingDatasetSizeLimit().intValue());
    }

    /**
     * Test minimum data points getter
     */
    public void testGetMinimumDatapoints() {
        MethodComponentContext methodComponent = new MethodComponentContext("test-method", Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.L1, methodComponent,
                null, null, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        assertEquals(KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1,
                knnMethodContext.getMinimumDatapoints().intValue());
    }

    /**
     * Test KNNMethodContext validation
     */
    public void testValidate() {
        // Check valid default - this should not throw any exception
        KNNMethodContext.DEFAULT.validate();

        // Check a valid nmslib method
        MethodComponentContext hnswMethod = new MethodComponentContext(METHOD_HNSW, Collections.emptyMap());
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.NMSLIB, SpaceType.L2, hnswMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        knnMethodContext.validate();

        // Check invalid parameter nmslib
        hnswMethod = new MethodComponentContext(METHOD_HNSW, ImmutableMap.of("invalid", 111));
        KNNMethodContext knnMethodContext1 = new KNNMethodContext(KNNEngine.NMSLIB, SpaceType.L2, hnswMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        expectThrows(ValidationException.class, knnMethodContext1::validate);

        // Check invalid method nmslib
        MethodComponentContext invalidMethod = new MethodComponentContext("invalid", Collections.emptyMap());
        KNNMethodContext knnMethodContext2 = new KNNMethodContext(KNNEngine.NMSLIB, SpaceType.L2, invalidMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        expectThrows(ValidationException.class, knnMethodContext2::validate);

        // Check valid method faiss
        MethodComponentContext ivfMethod = new MethodComponentContext(METHOD_IVF, Collections.emptyMap());
        knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, ivfMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        knnMethodContext.validate();

        // Check invalid method faiss
        KNNMethodContext knnMethodContext3 = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, invalidMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        expectThrows(ValidationException.class, knnMethodContext3::validate);

        // Check invalid coarse quantizer faiss
        KNNMethodContext coarseQuantizer = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, ivfMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        KNNMethodContext knnMethodContext4 = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, invalidMethod,
                coarseQuantizer, null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        expectThrows(ValidationException.class, knnMethodContext4::validate);

        // Check invalid encoder faiss
        MethodComponentContext encoderContext = new MethodComponentContext(ENCODER_PQ, Collections.emptyMap());
        KNNMethodContext knnMethodContext5 = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, invalidMethod,
                null, encoderContext,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        expectThrows(ValidationException.class, knnMethodContext5::validate);
    }

    /**
     * Test method generation
     */
    public void testGenerateMethod() {
        // Test default
        assertEquals(METHOD_HNSW, KNNMethodContext.DEFAULT.generateMethod());

        // Test nmslib - nmslib should just have hnsw name as the method
        int m = 12;
        int efConstruction = 44;
        MethodComponentContext hnswMethod = new MethodComponentContext(METHOD_HNSW,
                ImmutableMap.of(METHOD_PARAMETER_M, m, METHOD_PARAMETER_EF_CONSTRUCTION, efConstruction));
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.NMSLIB, SpaceType.L2, hnswMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals(METHOD_HNSW, knnMethodContext.generateMethod());

        // Test faiss hnsw
        knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, hnswMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals(KNNLibrary.Faiss.METHODS.get(METHOD_HNSW).getMethodComponent().getName()
                + m + "," + KNNLibrary.Faiss.ENCODERS.get(ENCODER_FLAT).getName(),
                knnMethodContext.generateMethod());

        // Test faiss complex mapping -> IVF20344(IVF43(HNSW12,Flat),PQ7),PQ100
        int ncentroids1 = 20344;
        int ncentroids2 = 43;
        int pq1 = 100;
        int pq2 = 7;

        MethodComponentContext quantizerMethodComponent = new MethodComponentContext(
                METHOD_IVF, ImmutableMap.of(METHOD_PARAMETER_NCENTROIDS, ncentroids2));

        MethodComponentContext encoder2 = new MethodComponentContext(
                ENCODER_PQ, ImmutableMap.of(METHOD_PARAMETER_CODE_SIZE, pq2));

        KNNMethodContext coarseQuantizer = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, quantizerMethodComponent,
                knnMethodContext, encoder2, KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        MethodComponentContext encoder1 = new MethodComponentContext(
                ENCODER_PQ, ImmutableMap.of(METHOD_PARAMETER_CODE_SIZE, pq1));
        MethodComponentContext ivfMethod = new MethodComponentContext(
                METHOD_IVF, ImmutableMap.of(METHOD_PARAMETER_NCENTROIDS, ncentroids1));
        knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, ivfMethod, coarseQuantizer,
                encoder1,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);
        assertEquals("IVF20344(IVF43(HNSW12,Flat),PQ7),PQ100", knnMethodContext.generateMethod());
    }

    /**
     * Test generation of extra parameters
     */
    public void testGenerateExtraParameters() throws IOException {
        int ncentroids = 20344;
        int nprobes = 43;

        Map<String, Object> extraParams = ImmutableMap.of(METHOD_PARAMETER_NPROBES, nprobes);

        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        Map<String, Object> params = builder
                .put(METHOD_PARAMETER_NCENTROIDS, ncentroids)
                .putAll(extraParams)
                .build();

        MethodComponentContext ivfMethod = new MethodComponentContext(METHOD_IVF, params);
        KNNMethodContext knnMethodContext = new KNNMethodContext(KNNEngine.FAISS, SpaceType.L2, ivfMethod, null,
                null,KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT + 1,
                KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

        assertEquals(Strings.toString(XContentFactory.jsonBuilder().map(extraParams)),
                knnMethodContext.generateExtraParameters());
    }

    /**
     * Test context method parsing when input is invalid
     */
    public void testParse_invalid() throws IOException {
        // Invalid input type
        Integer invalidIn = 12;
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(invalidIn, null, null));

        // Invalid engine type
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(KNN_ENGINE,0)
                .endObject();

        final Map<String, Object> in0 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in0, null, null));

        // Invalid engine name
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(KNN_ENGINE,"invalid")
                .endObject();

        final Map<String, Object> in1 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in1, null, null));

        // Invalid engine parent mismatch
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(KNN_ENGINE, KNNEngine.NMSLIB)
                .endObject();

        final Map<String, Object> in2 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in2, KNNEngine.FAISS, null));

        // Invalid space type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(SPACE_TYPE, 0)
                .endObject();

        final Map<String, Object> in3 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in3, null, null));

        // Invalid space name
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(SPACE_TYPE, "invalid")
                .endObject();

        final Map<String, Object> in4 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in4, null, null));

        // Invalid space parent mismatch
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .endObject();

        final Map<String, Object> in5 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in5, null, SpaceType.L2));

        // Invalid name not set
        xContentBuilder = XContentFactory.jsonBuilder().startObject().endObject();
        final Map<String, Object> in6 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in6, null, null));

        // Invalid name type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, 13)
                .endObject();

        final Map<String, Object> in7 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in7, null, null));

        // Invalid parameter type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(PARAMETERS, 13)
                .endObject();

        final Map<String, Object> in8 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in8, null, null));

        // Invalid training data set limit type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(TRAINING_DATASET_SIZE_LIMIT, "invalid")
                .endObject();

        final Map<String, Object> in9 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in9, null, null));

        // Invalid training data set limit value
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(TRAINING_DATASET_SIZE_LIMIT, -1)
                .endObject();

        final Map<String, Object> in10 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in10, null, null));

        // Invalid minimum data points type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(MINIMUM_DATAPOINTS, "invalid")
                .endObject();

        final Map<String, Object> in11 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in11, null, null));

        // Invalid minimum data points value
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(MINIMUM_DATAPOINTS, -1)
                .endObject();

        final Map<String, Object> in12 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in12, null, null));

        // Invalid coarse quantizer - name missing
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(KNN_ENGINE, KNNEngine.FAISS.getName())
                .field(NAME, METHOD_IVF)
                .startObject(COARSE_QUANTIZER)
                .endObject()
                .endObject();

        final Map<String, Object> in13 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> KNNMethodContext.parse(in13, null, null));

        // Invalid key
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("invalid", 12)
                .endObject();
        Map<String, Object> in14 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(in14));
    }

    /**
     * Test context method parsing when input is valid
     */
    public void testParse_valid() throws IOException {
        // Simple method with only name set
        String methodName = "test-method";

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext = KNNMethodContext.parse(in, null, null);

        assertEquals(KNNEngine.DEFAULT, knnMethodContext.getEngine());
        assertEquals(SpaceType.DEFAULT, knnMethodContext.getSpaceType());
        assertEquals(methodName, knnMethodContext.getMethodComponent().getName());
        assertNull(knnMethodContext.getMethodComponent().getParameters());

        // Method with parameters
        String methodParameterKey1 = "p-1";
        String methodParameterValue1 = "v-1";
        String methodParameterKey2 = "p-2";
        Integer methodParameterValue2 = 27;

        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(PARAMETERS)
                .field(methodParameterKey1, methodParameterValue1)
                .field(methodParameterKey2, methodParameterValue2)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);

        assertEquals(methodParameterValue1,
                knnMethodContext.getMethodComponent().getParameters().get(methodParameterKey1));
        assertEquals(methodParameterValue2,
                knnMethodContext.getMethodComponent().getParameters().get(methodParameterKey2));

        // Method w/ encoder
        String encoderName = "test-encoder";
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(ENCODER)
                .field(NAME, encoderName)
                .startObject(PARAMETERS)
                .field(methodParameterKey1, methodParameterValue1)
                .field(methodParameterKey2, methodParameterValue2)
                .endObject()
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);

        assertEquals(methodParameterValue1,
                knnMethodContext.getEncoder().getParameters().get(methodParameterKey1));
        assertEquals(methodParameterValue2,
                knnMethodContext.getEncoder().getParameters().get(methodParameterKey2));

        // Method w/ coarse quantizer
        String quantizerName = "test-quantizer";
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(COARSE_QUANTIZER)
                .field(NAME, quantizerName)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);

        assertEquals(quantizerName, knnMethodContext.getCoarseQuantizer().getMethodComponent().getName());
    }

    /**
     * Test toXContent method
     */
    public void testToXContent() throws IOException {
        // Method w/ coarse quantizer and w/ encoder
        String methodName = "test-method";
        String spaceType = SpaceType.L2.getValue();
        String knnEngine = KNNEngine.DEFAULT.getName();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(SPACE_TYPE, spaceType)
                .field(KNN_ENGINE, knnEngine)
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext = KNNMethodContext.parse(in, null, null);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = knnMethodContext.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        Map<String, Object> out = xContentBuilderToMap(builder);
        assertEquals(methodName, out.get(NAME));
        assertEquals(spaceType, out.get(SPACE_TYPE));
        assertEquals(knnEngine, out.get(KNN_ENGINE));

        // Test with coarse quantizer
        String coarseQuantizer = "test-quantizer";
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(COARSE_QUANTIZER)
                .field(NAME, coarseQuantizer)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);

        builder = XContentFactory.jsonBuilder().startObject();
        builder = knnMethodContext.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        out = xContentBuilderToMap(builder);

        @SuppressWarnings("unchecked")
        Map<String, Object> coarseQuantizerMap = (Map<String, Object>) out.get(COARSE_QUANTIZER);

        assertEquals(coarseQuantizer, coarseQuantizerMap.get(NAME));

        // Test encoder
        // Test with coarse quantizer
        String encoder = "test-encoder";
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(ENCODER)
                .field(NAME, encoder)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);

        builder = XContentFactory.jsonBuilder().startObject();
        builder = knnMethodContext.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        out = xContentBuilderToMap(builder);

        @SuppressWarnings("unchecked")
        Map<String, Object> encoderMap = (Map<String, Object>) out.get(ENCODER);

        assertEquals(encoder, encoderMap.get(NAME));
    }
}
