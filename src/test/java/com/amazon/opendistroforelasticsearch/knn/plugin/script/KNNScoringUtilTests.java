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

package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;

import java.util.BitSet;

public class KNNScoringUtilTests extends KNNTestCase {

    public void testL2SquaredScoringFunction() {
        float[] queryVector = {1.0f, 1.0f, 1.0f};
        float[] inputVector = {4.0f, 4.0f, 4.0f};

        Float distance = KNNScoringUtil.l2Squared(queryVector, inputVector);
        assertTrue(distance == 27.0f);
    }

    public void testCosineSimilScoringFunction() {
        float[] queryVector = {1.0f, 1.0f, 1.0f};
        float[] inputVector = {4.0f, 4.0f, 4.0f};

        float queryVectorMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(queryVector);
        float inputVectorMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(inputVector);
        float dotProduct = 12.0f;
        float expectedScore = (float) (dotProduct / (Math.sqrt(queryVectorMagnitude * inputVectorMagnitude)));


        Float actualScore = KNNScoringUtil.cosinesimil(queryVector, inputVector);
        assertEquals(expectedScore, actualScore, 0.0001);
    }

    public void testCosineSimilOptimizedScoringFunction() {
        float[] queryVector = {1.0f, 1.0f, 1.0f};
        float[] inputVector = {4.0f, 4.0f, 4.0f};
        float queryVectorMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(queryVector);
        float inputVectorMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(inputVector);
        float dotProduct = 12.0f;
        float expectedScore = (float) (dotProduct / (Math.sqrt(queryVectorMagnitude * inputVectorMagnitude)));

        Float actualScore = KNNScoringUtil.cosinesimilOptimized(queryVector, inputVector, queryVectorMagnitude);
        assertEquals(expectedScore, actualScore, 0.0001);
    }

    public void testGetInvalidVectorMagnitudeSquared() {
        float[] queryVector = null;
        // vector cannot be null
        expectThrows(IllegalStateException.class, () -> KNNScoringUtil.getVectorMagnitudeSquared(queryVector));
    }

    public void testConvertInvalidVectorToPrimitive() {
        float[] primitiveVector = null;
        assertEquals(primitiveVector, KNNScoringUtil.convertVectorToPrimitive(primitiveVector));
    }

    public void testCosineSimilQueryVectorZeroMagnitude() {
        float[] queryVector = {0,0};
        float[] inputVector = {4.0f, 4.0f};
        assertEquals(Float.MIN_VALUE, KNNScoringUtil.cosinesimil(queryVector, inputVector), 0.00001);
    }

    public void testCosineSimilOptimizedQueryVectorZeroMagnitude() {
        float[] inputVector = {4.0f, 4.0f};
        float[] queryVector = {0, 0};
        assertTrue(Float.MIN_VALUE == KNNScoringUtil.cosinesimilOptimized(queryVector, inputVector, 0.0f));
    }

    public void testBitHammingDistance_BitSet() {
        BitSet bitSet1 = new BitSet(10);
        BitSet bitSet2 = new BitSet(10);
        BitSet bitSet3 = new BitSet(4);

        bitSet1.set(0);
        bitSet1.set(3);
        bitSet1.set(4);
        bitSet1.set(9);

        bitSet2.set(0);
        bitSet2.set(2);
        bitSet2.set(4);

        bitSet3.set(0);
        bitSet3.set(3);

        assertEquals(3.0, KNNScoringUtil.bitHamming(bitSet1, bitSet2), 0.1);
        assertEquals(2.0, KNNScoringUtil.bitHamming(bitSet1, bitSet3), 0.1);
    }

    public void testBitHammingDistance_Long() {
        // 64 bit 2's complement:
        // 1_817L                   = 00000000_00000000_00000000_00000000_00000000_00000000_00000111_00011001
        // 500_000_924_849_631L     = 00000000_00000001_11000110_10111111_10001001_10000011_01010101_11011111
        // -500_000_924_849_631L    = 11111111_11111110_00111001_01000000_01110110_01111100_10101010_00100001
        Long long1 = 1_817L;
        Long long2 = 500_000_924_849_631L;
        Long long3 = -500_000_924_849_631L;

        assertEquals(25.0, KNNScoringUtil.bitHamming(long1, long2), 0.1);
        assertEquals(38.0, KNNScoringUtil.bitHamming(long1, long3), 0.1);
        assertEquals(63.0, KNNScoringUtil.bitHamming(long2, long3), 0.1);
        assertEquals(0.0, KNNScoringUtil.bitHamming(long3, long3), 0.1);
    }
}
