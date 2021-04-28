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
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;

public class MethodComponentTests extends KNNTestCase {
    /**
     * Test name getter
     */
    public void testGetName() {
        String name = "test";
        MethodComponent methodComponent = new MethodComponent.Builder(name).build();
        assertEquals(name, methodComponent.getName());
    }

    /**
     * Test parameter getter
     */
    public void testGetParameters() {
        String name = "test";
        String paramKey = "key";
        MethodComponent methodComponent = new MethodComponent.Builder(name)
                .putParameter(paramKey, new Parameter.IntegerParameter(1, false, v -> v > 0))
                .build();
        assertEquals(1, methodComponent.getParameters().size());
        assertTrue(methodComponent.getParameters().containsKey(paramKey));
    }

    /**
     * Test generate extra parameter map
     */
    public void testGenerateExtraParameterMap() throws IOException {
        String methodName = "test-name";
        String key1 = "test-key-1";
        String key2 = "test-key-2";
        MethodComponent methodComponent = new MethodComponent.Builder(methodName)
                .putParameter(key1, new Parameter.IntegerParameter(1, false, v -> v > 0))
                .putParameter(key2, new Parameter.IntegerParameter(1, true, v -> v > 0))
                .build();

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
     * Test validation
     */
    public void testValidate() throws IOException {
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

        MethodComponent methodComponent1 = new MethodComponent.Builder(methodName).build();

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

        MethodComponent methodComponent2 = new MethodComponent.Builder(methodName)
                .putParameter("valid", new Parameter.IntegerParameter(1, false, v -> v > 0))
                .build();

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

        MethodComponent methodComponent3 = new MethodComponent.Builder(methodName)
                .putParameter("valid1", new Parameter.IntegerParameter(1, false, v -> v > 0))
                .putParameter("valid2", new Parameter.IntegerParameter(1, false, v -> v > 0))
                .build();
        methodComponent3.validate(componentContext3);

        // valid configuration - empty parameters
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, methodName)
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        KNNMethodContext.MethodComponentContext componentContext4 = KNNMethodContext.MethodComponentContext.parse(in);

        MethodComponent methodComponent4 = new MethodComponent.Builder(methodName)
                .putParameter("valid1", new Parameter.IntegerParameter(1, false, v -> v > 0))
                .putParameter("valid2", new Parameter.IntegerParameter(1, false, v -> v > 0))
                .build();
        methodComponent4.validate(componentContext4);
    }
}
