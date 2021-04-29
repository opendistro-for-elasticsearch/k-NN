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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;

public class MethodComponentContextTests extends KNNTestCase {
    /**
     * Test parse where input is invalid
     */
    public void testParse_invalid() throws IOException {
        // Input is not a Map
        Integer invalidIn = 12;
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(invalidIn));

        // Name not passed in
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().endObject();
        Map<String, Object> in0 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(in0));

        // Invalid name type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, 12)
                .endObject();
        Map<String, Object> in1 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(in1));

        // Invalid parameter type
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(PARAMETERS, 12)
                .endObject();
        Map<String, Object> in2 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(in2));

        // Invalid key
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("invalid", 12)
                .endObject();
        Map<String, Object> in3 = xContentBuilderToMap(xContentBuilder);
        expectThrows(MapperParsingException.class, () -> MethodComponentContext.parse(in3));
    }

    /**
     * Test name getter
     */
    public void testGetName() {
        String name = "test-name";
        MethodComponentContext methodContext = new MethodComponentContext(name, null);
        assertEquals(name, methodContext.getName());
    }


    /**
     * Test parameters getter
     */
    public void testGetParameters() throws IOException {
        String name = "test-name";
        String paramKey1 = "p-1";
        String paramVal1 = "v-1";
        String paramKey2 = "p-2";
        Integer paramVal2 = 1;
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(paramKey1, paramVal1)
                .field(paramKey2, paramVal2)
                .endObject();
        Map<String, Object> params = xContentBuilderToMap(xContentBuilder);
        MethodComponentContext methodContext = new MethodComponentContext(name, params);
        assertEquals(paramVal1, methodContext.getParameters().get(paramKey1));
        assertEquals(paramVal2, methodContext.getParameters().get(paramKey2));
    }

    /**
     * Test parse where input is valid
     */
    public void testParse_valid() throws IOException {
        // Empty parameters
        String name = "test-name";
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, name)
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        MethodComponentContext methodContext = MethodComponentContext.parse(in);
        assertEquals(name, methodContext.getName());
        assertNull(methodContext.getParameters());

        // Multiple parameters
        String paramKey1 = "p-1";
        String paramVal1 = "v-1";
        String paramKey2 = "p-2";
        Integer paramVal2 = 1;

        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, name)
                .startObject(PARAMETERS)
                .field(paramKey1, paramVal1)
                .field(paramKey2, paramVal2)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        methodContext = MethodComponentContext.parse(in);

        assertEquals(paramVal1, methodContext.getParameters().get(paramKey1));
        assertEquals(paramVal2, methodContext.getParameters().get(paramKey2));
    }

    /**
     * Test  toXContent
     */
    public void testToXContent() throws IOException {
        // Empty parameters
        String name = "test-name";
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, name)
                .endObject();
        Map<String, Object> in = xContentBuilderToMap(xContentBuilder);
        MethodComponentContext methodContext = MethodComponentContext.parse(in);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder = methodContext.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        Map<String, Object> out = xContentBuilderToMap(builder);
        assertEquals(name, out.get(NAME));


        // Multiple parameters
        String paramKey1 = "p-1";
        String paramVal1 = "v-1";
        String paramKey2 = "p-2";
        Integer paramVal2 = 1;
        xContentBuilder = XContentFactory.jsonBuilder().startObject()
                .field(NAME, name)
                .startObject(PARAMETERS)
                .field(paramKey1, paramVal1)
                .field(paramKey2, paramVal2)
                .endObject()
                .endObject();
        in = xContentBuilderToMap(xContentBuilder);
        methodContext = MethodComponentContext.parse(in);

        builder = XContentFactory.jsonBuilder().startObject();
        builder = methodContext.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();

        out = xContentBuilderToMap(builder);

        @SuppressWarnings("unchecked")
        Map<String, Object> paramMap = (Map<String, Object>) out.get(PARAMETERS);

        assertEquals(paramVal1, paramMap.get(paramKey1));
        assertEquals(paramVal2, paramMap.get(paramKey2));
    }
}
