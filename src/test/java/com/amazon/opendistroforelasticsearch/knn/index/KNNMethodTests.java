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
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .setIsCoarseQuantizerAvailable(false)
                .build();
        assertEquals(name, knnMethod.getMethodComponent().getName());
    }

    /**
     * Test KNNMethod has space
     */
    public void testHasSpace() {
        String name = "test";
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .setIsCoarseQuantizerAvailable(false)
                .addSpaces(SpaceType.L2, SpaceType.COSINESIMIL)
                .build();
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
        Map<String, MethodComponent> encoders = ImmutableMap.of(
                encoder, MethodComponent.Builder.builder(encoder).build()
        );

        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .putEncoders(encoders)
                .setIsCoarseQuantizerAvailable(false)
                .build();

        assertEquals(encoder, knnMethod.getEncoder(encoder).getName());
    }

    /**
     * Test KNNMethod has encoder
     */
    public void testHasEncoder() {
        String name = "test";
        String encoder = "test-encoder";
        Map<String, MethodComponent> encoders = ImmutableMap.of(
                encoder, MethodComponent.Builder.builder(encoder).build()
        );

        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .putEncoders(encoders)
                .setIsCoarseQuantizerAvailable(false)
                .build();
        assertTrue(knnMethod.hasEncoder(encoder));
        assertFalse(knnMethod.hasEncoder("invalid"));
    }

    /**
     * Test KNNMethod is coarse quantizer available
     */
    public void testIsCoarseQuantizerAvailable() {
        String name = "test";
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .setIsCoarseQuantizerAvailable(false)
                .build();
        assertFalse(knnMethod.isCoarseQuantizerAvailable());
        knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build())
                .setIsCoarseQuantizerAvailable(true)
                .build();
        assertTrue(knnMethod.isCoarseQuantizerAvailable());
    }

    /**
     * Test KNNMethod validate
     */
    public void testValidate() throws IOException {
        String methodName = "test-method";
        Set<SpaceType> spaceTypeSet = ImmutableSet.of(SpaceType.L2);
        String encoderName = "enc-1";
        Map<String, MethodComponent> encoders = ImmutableMap.of(encoderName,
                MethodComponent.Builder.builder(encoderName).build());
        KNNMethod knnMethod = KNNMethod.Builder.builder(MethodComponent.Builder.builder(methodName).build())
                .addSpaces(SpaceType.L2)
                .putEncoders(encoders)
                .setIsCoarseQuantizerAvailable(false)
                .build();

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

    public void testBuilder() {
        String name = "test";
        KNNMethod.Builder builder = KNNMethod.Builder.builder(MethodComponent.Builder.builder(name).build());
        KNNMethod knnMethod = builder.build();

        assertEquals(name, knnMethod.getMethodComponent().getName());

        builder.addSpaces(SpaceType.L2);
        knnMethod = builder.build();

        assertTrue(knnMethod.hasSpace(SpaceType.L2));

        String encoder = "test-encoder";
        Map<String, MethodComponent> encoders = ImmutableMap.of(
                encoder, MethodComponent.Builder.builder(encoder).build()
        );
        builder.putEncoders(encoders);
        knnMethod = builder.build();

        assertEquals(encoder, knnMethod.getEncoder(encoder).getName());

        builder.setIsCoarseQuantizerAvailable(true);
        knnMethod = builder.build();
        assertTrue(knnMethod.isCoarseQuantizerAvailable());
    }
}
