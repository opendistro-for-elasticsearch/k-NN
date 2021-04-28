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
        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), Collections.emptySet(),
                Collections.emptyMap(), false);
        assertEquals(name, knnMethod.getMethodComponent().getName());
    }

    /**
     * Test KNNMethod has space
     */
    public void testHasSpace() {
        String name = "test";
        Set<SpaceType> spaceTypeSet = ImmutableSet.of(SpaceType.L2, SpaceType.COSINESIMIL);
        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), spaceTypeSet,
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
        Map<String, MethodComponent> encoders = ImmutableMap.of(
                encoder, new MethodComponent.Builder(encoder).build()
        );

        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), Collections.emptySet(), encoders,
                false);
        assertEquals(encoder, knnMethod.getEncoder(encoder).getName());
    }

    /**
     * Test KNNMethod has encoder
     */
    public void testHasEncoder() {
        String name = "test";
        String encoder = "test-encoder";
        Map<String, MethodComponent> encoders = ImmutableMap.of(
                encoder, new MethodComponent.Builder(encoder).build()
        );

        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), Collections.emptySet(), encoders,
                false);
        assertTrue(knnMethod.hasEncoder(encoder));
        assertFalse(knnMethod.hasEncoder("invalid"));
    }

    /**
     * Test KNNMethod is coarse quantizer available
     */
    public void testIsCoarseQuantizerAvailable() {
        String name = "test";
        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), Collections.emptySet(),
                Collections.emptyMap(), false);
        assertFalse(knnMethod.isCoarseQuantizerAvailable());
        knnMethod = new KNNMethod(new MethodComponent.Builder(name).build(), Collections.emptySet(),
                Collections.emptyMap(), true);
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
                new MethodComponent.Builder(encoderName).build());
        KNNMethod knnMethod = new KNNMethod(new MethodComponent.Builder(methodName).build(), spaceTypeSet, encoders,
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
}
