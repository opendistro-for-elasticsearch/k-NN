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

package com.amazon.opendistroforelasticsearch.knn.index.util;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethod;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethodContext;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_FLAT;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_PQ;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_IVF;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_CODE_SIZE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NCENTROIDS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;

public class KNNLibraryTests extends KNNTestCase {
    /**
     * Test native library build version getter
     */
    public void testNativeLibrary_getLatestBuildVersion() {
        String latestBuildVersion = "test-build-version";
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(Collections.emptyMap(), Collections.emptyMap(),
                latestBuildVersion, "", "");
        assertEquals(latestBuildVersion, testNativeLibrary.getLatestBuildVersion());
    }

    /**
     * Test native library version getter
     */
    public void testNativeLibrary_getLatestLibVersion() {
        String latestVersion = "test-lib-version";
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(Collections.emptyMap(), Collections.emptyMap(),
                "", latestVersion, "");
        assertEquals(latestVersion, testNativeLibrary.getLatestLibVersion());
    }

    /**
     * Test native library extension getter
     */
    public void testNativeLibrary_getExtension() {
        String extension = ".extension";
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(Collections.emptyMap(), Collections.emptyMap(),
                "", "", extension);
        assertEquals(extension, testNativeLibrary.getExtension());
    }

    /**
     * Test native library compound extension getter
     */
    public void testNativeLibrary_getCompoundExtension() {
        String extension = ".extension";
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(Collections.emptyMap(), Collections.emptyMap(),
                "", "", extension);
        assertEquals(extension + "c", testNativeLibrary.getCompoundExtension());
    }

    /**
     * Test native library compound extension getter
     */
    public void testNativeLibrary_getMethod() {
        String methodName1 = "test-method-1";
        KNNMethod knnMethod1 = new KNNMethod(methodName1, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);
        String methodName2 = "test-method-2";
        KNNMethod knnMethod2 = new KNNMethod(methodName2, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);

        Map<String, KNNMethod> knnMethodMap = ImmutableMap.of(
                methodName1, knnMethod1, methodName2, knnMethod2
        );

        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(knnMethodMap, Collections.emptyMap(),
                "", "", "");
        assertEquals(knnMethod1, testNativeLibrary.getMethod(methodName1));
        assertEquals(knnMethod2, testNativeLibrary.getMethod(methodName2));
        expectThrows(IllegalArgumentException.class, () -> testNativeLibrary.getMethod("invalid"));
    }

    /**
     * Test native library scoring override
     */
    public void testNativeLibrary_score() {
        Map<SpaceType, Function<Float, Float>> translationMap = ImmutableMap.of(SpaceType.L2, s -> s*2);
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(Collections.emptyMap(), translationMap,
                "", "", "");
        // Test override
        assertEquals(2f, testNativeLibrary.score(1f, SpaceType.L2), 0.0001);

        // Test non-override
        assertEquals(SpaceType.L1.scoreTranslation(1f), testNativeLibrary.score(1f, SpaceType.L1), 0.0001);
    }

    /**
     * Test native library method validation
     */
    public void testNativeLibrary_validateMethod() throws IOException {
        // Invalid - method not supported
        String methodName1 = "test-method-1";
        KNNMethod knnMethod1 = new KNNMethod(methodName1, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);

        Map<String, KNNMethod> methodMap = ImmutableMap.of(methodName1, knnMethod1);
        TestNativeLibrary testNativeLibrary1 = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, "invalid")
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext1 = KNNMethodContext.parse(in, null, null);
        expectThrows(ValidationException.class, ()-> testNativeLibrary1.validateMethod(knnMethodContext1));

        // Invalid - method validation
        String methodName2 = "test-method-2";
        KNNMethod knnMethod2 = new KNNMethod(methodName2, Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(), false) {
            @Override
            public void validate(KNNMethodContext knnMethodContext) {
                throw new ValidationException();
            }
        };

        methodMap = ImmutableMap.of(methodName2, knnMethod2);
        TestNativeLibrary testNativeLibrary2 = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName2)
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext2 = KNNMethodContext.parse(in, null, null);
        expectThrows(ValidationException.class, ()-> testNativeLibrary2.validateMethod(knnMethodContext2));

        // Invalid - coarse quantizer
        String methodName3 = "test-method-3";
        KNNMethod knnMethod3 = new KNNMethod(methodName3, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), true) {
            @Override
            public void validate(KNNMethodContext knnMethodContext) {}
        };

        methodMap = ImmutableMap.of(methodName3, knnMethod3);
        TestNativeLibrary testNativeLibrary3 = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");

        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName3)
                .startObject(COARSE_QUANTIZER)
                .field(NAME, "invalid")
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext3 = KNNMethodContext.parse(in, null, null);
        expectThrows(ValidationException.class, ()-> testNativeLibrary3.validateMethod(knnMethodContext3));

        // Valid coarse quantizer
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName3)
                .startObject(COARSE_QUANTIZER)
                .field(NAME, methodName3)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext4 = KNNMethodContext.parse(in, null, null);
        testNativeLibrary3.validateMethod(knnMethodContext4);
    }

    /**
     * Test native library method generation
     */
    public void testNativeLibrary_generateMethod() throws IOException {
        String mappingMethodName = "TEST-METHOD-1";
        String methodName = "test-method-1";
        KNNMethod knnMethod1 = new KNNMethod(methodName, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);

        Map<String, KNNMethod> methodMap = ImmutableMap.of(mappingMethodName, knnMethod1);
        TestNativeLibrary testNativeLibrary = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, mappingMethodName)
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals(methodName, testNativeLibrary.generateMethod(knnMethodContext));
    }

    /**
     * Test native library extra parameter generation
     */
    public void testNativeLibrary_generateExtraParameterMap() throws IOException {
        // Invalid method
        String methodName1 = "test-method-1";
        KNNMethod knnMethod1 = new KNNMethod(methodName1, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);

        Map<String, KNNMethod> methodMap = ImmutableMap.of(methodName1, knnMethod1);
        TestNativeLibrary testNativeLibrary1 = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, "invalid")
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext1 = KNNMethodContext.parse(in, null, null);
        expectThrows(IllegalArgumentException.class,
                () -> testNativeLibrary1.generateExtraParameterMap(knnMethodContext1));

        // Extra parameters
        String methodName2 = "test-method-2";
        String key = "test-key";
        String value = "test-value";

        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(key, value)
                .endObject();
        Map<String, Object> inParams = xContentBuilderToMap(xContentBuilder);
        KNNMethod knnMethod2 = new KNNMethod(methodName2, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), true) {
            @Override
            public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
                return inParams;
            }
        };
        methodMap = ImmutableMap.of(methodName2, knnMethod2);
        TestNativeLibrary testNativeLibrary2 = new TestNativeLibrary(methodMap, Collections.emptyMap(),
                "", "", "");
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName2)
                .startObject(COARSE_QUANTIZER)
                .field(NAME, methodName2)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext2 = KNNMethodContext.parse(in, null, null);
        assertEquals(inParams, testNativeLibrary2.generateExtraParameterMap(knnMethodContext2));
    }

    /**
     * Test faiss method generation method
     */
    public void testFaiss_generateMethod() throws IOException {
        // HNSW32,Flat
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_M, 32)
                .endObject()
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals("HNSW32,Flat", KNNLibrary.Faiss.INSTANCE.generateMethod(knnMethodContext));

        // HNSW32,Flat (again with encoder)
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_M, 32)
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_FLAT)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals("HNSW32,Flat", KNNLibrary.Faiss.INSTANCE.generateMethod(knnMethodContext));

        // HNSW32,PQ8
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_M, 32)
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, 8)
                .endObject()
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals("HNSW32,PQ8", KNNLibrary.Faiss.INSTANCE.generateMethod(knnMethodContext));

        // IVF16,PQ163
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NCENTROIDS, 16)
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, 163)
                .endObject()
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals("IVF16,PQ163", KNNLibrary.Faiss.INSTANCE.generateMethod(knnMethodContext));

        // IVF128000(IVF16(HNSW32,Flat),PQ16),PQ127
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NCENTROIDS, 128000)
                .endObject()
                .startObject(COARSE_QUANTIZER)
                .field(NAME, METHOD_IVF)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_NCENTROIDS, 16)
                .endObject()
                .startObject(COARSE_QUANTIZER)
                .field(NAME, METHOD_HNSW)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_M, 32)
                .endObject()
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, 16)
                .endObject()
                .endObject()
                .endObject()
                .startObject(ENCODER)
                .field(NAME, ENCODER_PQ)
                .startObject(PARAMETERS)
                .field(METHOD_PARAMETER_CODE_SIZE, 127)
                .endObject()
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        knnMethodContext = KNNMethodContext.parse(in, null, null);
        assertEquals("IVF128000(IVF16(HNSW32,Flat),PQ16),PQ127",
                KNNLibrary.Faiss.INSTANCE.generateMethod(knnMethodContext));
    }

    static class TestNativeLibrary extends KNNLibrary.NativeLibrary {
        /**
         * Constructor for TestNativeLibrary
         *
         * @param methods map of methods the native library supports
         * @param scoreTranslation Map of translation of space type to scores returned by the library
         * @param latestLibraryBuildVersion String representation of latest build version of the library
         * @param latestLibraryVersion String representation of latest version of the library
         * @param extension String representing the extension that library files should use
         */
        public TestNativeLibrary(Map<String, KNNMethod> methods,
                                 Map<SpaceType, Function<Float, Float>> scoreTranslation,
                                 String latestLibraryBuildVersion, String latestLibraryVersion, String extension) {
            super(methods, scoreTranslation, latestLibraryBuildVersion, latestLibraryVersion, extension);
        }
    }
}
