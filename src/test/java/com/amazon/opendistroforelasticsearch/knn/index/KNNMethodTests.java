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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.SPACE_TYPE;


public class KNNMethodTests extends KNNTestCase {
    /**
     * Test KNNMethod method component getter
     */
    public void testGetMethodComponent() {
        String name = "test";
        KNNMethod knnMethod = new KNNMethod(name, Collections.emptySet(), Collections.emptyMap(),
                Collections.emptyMap(), false);
        assertEquals(name, knnMethod.getMethodComponent().getName());
    }

    /**
     * Test KNNMethod has space
     */
    public void testHasSpace() {
        String name = "test";
        Set<SpaceType> spaceTypeSet = ImmutableSet.of(SpaceType.L2, SpaceType.COSINESIMIL);
        KNNMethod knnMethod = new KNNMethod(name, spaceTypeSet, Collections.emptyMap(),
                Collections.emptyMap(), false);
        assertTrue(knnMethod.hasSpace(SpaceType.L2));
        assertTrue(knnMethod.hasSpace(SpaceType.COSINESIMIL));
        assertFalse(knnMethod.hasSpace(SpaceType.INNER_PRODUCT));
    }

    /**
     * Test KNNMethod encoder getter
     */
    public void testGetEncoder() {
        String name = "test";
        String encoder = "test-encoder";
        Map<String, KNNMethod.MethodComponent> encoders = ImmutableMap.of(
                encoder, new KNNMethod.MethodComponent(encoder, Collections.emptyMap())
        );

        KNNMethod knnMethod = new KNNMethod(name, Collections.emptySet(), Collections.emptyMap(), encoders, false);
        assertEquals(encoder, knnMethod.getEncoder(encoder).getName());
    }

    /**
     * Test KNNMethod has encoder
     */
    public void testHasEncoder() {
        String name = "test";
        String encoder = "test-encoder";
        Map<String, KNNMethod.MethodComponent> encoders = ImmutableMap.of(
                encoder, new KNNMethod.MethodComponent(encoder, Collections.emptyMap())
        );

        KNNMethod knnMethod = new KNNMethod(name, Collections.emptySet(), Collections.emptyMap(), encoders, false);
        assertTrue(knnMethod.hasEncoder(encoder));
        assertFalse(knnMethod.hasEncoder("invalid"));
    }

    /**
     * Test KNNMethod is coarse quantizer available
     */
    public void testIsCoarseQuantizerAvailable() {
        String name = "test";
        KNNMethod knnMethod = new KNNMethod(name, Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(),
                false);
        assertFalse(knnMethod.isCoarseQuantizerAvailable());
        knnMethod = new KNNMethod(name, Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(), true);
        assertTrue(knnMethod.isCoarseQuantizerAvailable());
    }

    /**
     * Test KNNMethod validate
     */
    public void testValidate() throws IOException {
        String methodName = "test-method";
        Set<SpaceType> spaceTypeSet = ImmutableSet.of(SpaceType.L2);
        String encoderName = "enc-1";
        Map<String, KNNMethod.MethodComponent> encoders = ImmutableMap.of(encoderName,
                new KNNMethod.MethodComponent(encoderName, Collections.emptyMap()));
        KNNMethod knnMethod = new KNNMethod(methodName, spaceTypeSet, Collections.emptyMap(), encoders,
                false);

        // Invalid space
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(SPACE_TYPE, SpaceType.INNER_PRODUCT.getValue())
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext1 = KNNMethodContext.parse(in, null, null);

        expectThrows(ValidationException.class, () -> knnMethod.validate(knnMethodContext1));

        // Invalid methodComponent
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(SPACE_TYPE, SpaceType.L2.getValue())
                .startObject(PARAMETERS)
                .field("invalid", "invalid")
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext2 = KNNMethodContext.parse(in, null, null);

        expectThrows(ValidationException.class, () -> knnMethod.validate(knnMethodContext2));

        // Invalid encoder
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(SPACE_TYPE, SpaceType.L2.getValue())
                .startObject(ENCODER)
                .field(NAME, "invalid")
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext3 = KNNMethodContext.parse(in, null, null);

        expectThrows(ValidationException.class, () -> knnMethod.validate(knnMethodContext3));

        // Valid everything
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .field(SPACE_TYPE, SpaceType.L2.getValue())
                .startObject(ENCODER)
                .field(NAME, encoderName)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext knnMethodContext4 = KNNMethodContext.parse(in, null, null);
        knnMethod.validate(knnMethodContext4);
    }

    /**
     * Test Method Component name getter
     */
    public void testMethodComponent_getName() {
        String name = "test";
        KNNMethod.MethodComponent methodComponent = new KNNMethod.MethodComponent(name, Collections.emptyMap());
        assertEquals(name, methodComponent.getName());
    }

    /**
     * Test Method Component parameter getter
     */
    public void testMethodComponent_getParameters() {
        String name = "test";
        String paramKey = "key";
        Map<String, KNNMethod.Parameter<?>> parameterMap = ImmutableMap.of(
                paramKey, new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0)
        );
        KNNMethod.MethodComponent methodComponent = new KNNMethod.MethodComponent(name, parameterMap);
        assertEquals(parameterMap, methodComponent.getParameters());
    }

    /**
     * Test Method Component generate extra parameter map
     */
    public void testMethodComponent_generateExtraParameterMap() throws IOException {
        String methodName = "test-name";
        String key1 = "test-key-1";
        String key2 = "test-key-2";
        Map<String, KNNMethod.Parameter<?>> parameters = ImmutableMap.of(
                key1, new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0),
                key2, new KNNMethod.Parameter.IntegerParameter(1, true, v -> v > 0));
        KNNMethod.MethodComponent methodComponent = new KNNMethod.MethodComponent(methodName, parameters);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(PARAMETERS)
                .field(key1, 10)
                .field(key2, 10)
                .endObject()
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext = KNNMethodContext.MethodComponentContext.parse(in);

        Map<String, Object> extraParams = methodComponent.generateExtraParameterMap(componentContext.getParameters());
        assertEquals(1, extraParams.size());
        assertEquals(10, extraParams.get(key1));
        assertNull(extraParams.get(key2));
    }

    /**
     * Test Method Component parameter validation
     */
    public void testMethodComponent_validate() throws IOException {
        // Invalid parameter key
        String methodName = "test-method";
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(PARAMETERS)
                .field("invalid", "invalid")
                .endObject()
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext1 = KNNMethodContext.MethodComponentContext.parse(in);

        KNNMethod.MethodComponent methodComponent1 = new KNNMethod.MethodComponent(methodName, Collections.emptyMap());

        expectThrows(ValidationException.class, () -> methodComponent1.validate(componentContext1));

        // Invalid parameter type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(PARAMETERS)
                .field("valid", "invalid")
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext2 = KNNMethodContext.MethodComponentContext.parse(in);

        KNNMethod.MethodComponent methodComponent2 = new KNNMethod.MethodComponent(methodName, ImmutableMap.of("valid",
                new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0)));

        expectThrows(ValidationException.class, () -> methodComponent2.validate(componentContext2));

        // valid configuration
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .startObject(PARAMETERS)
                .field("valid1", 16)
                .field("valid2", 128)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext3 = KNNMethodContext.MethodComponentContext.parse(in);

        KNNMethod.MethodComponent methodComponent3 = new KNNMethod.MethodComponent(methodName, ImmutableMap.of(
                "valid1",
                new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0),
                "valid2",
                new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0)));
        methodComponent3.validate(componentContext3);

        // valid configuration - empty parameters
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext4 = KNNMethodContext.MethodComponentContext.parse(in);

        KNNMethod.MethodComponent methodComponent4 = new KNNMethod.MethodComponent(methodName, ImmutableMap.of(
                "valid1",
                new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0),
                "valid2",
                new KNNMethod.Parameter.IntegerParameter(1, false, v -> v > 0)));
        methodComponent4.validate(componentContext4);
    }


    /**
     * Test default default value getter
     */
    public void testParameter_getDefaultValue() {
        String defaultValue = "test-default";
        KNNMethod.Parameter<String> parameter = new KNNMethod.Parameter<String>(defaultValue, false, v -> true) {
            @Override
            public void validate(Object value) {}
        };

        assertEquals(defaultValue, parameter.getDefaultValue());
    }


    /**
     * Test default is in method string
     */
    public void testParameter_isInMethodString() {
        boolean inMethodString = false;
        KNNMethod.Parameter<String> parameter = new KNNMethod.Parameter<String>("", inMethodString, v -> true) {
            @Override
            public void validate(Object value) {}
        };

        assertEquals(inMethodString, parameter.isInMethodString());
    }

    /**
     * Test integer parameter validate
     */
    public void testIntegerParameter_validate() {
        final KNNMethod.Parameter.IntegerParameter parameter = new KNNMethod.Parameter.IntegerParameter(1, false,
                v -> v > 0);

        // Invalid type
        expectThrows(ValidationException.class, () -> parameter.validate("String"));

        // Invalid value
        expectThrows(ValidationException.class, () -> parameter.validate(-1));

        // valid value
        parameter.validate(12);
    }
}
