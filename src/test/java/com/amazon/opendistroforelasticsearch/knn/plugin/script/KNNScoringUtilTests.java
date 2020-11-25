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

import java.math.BigInteger;

import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.convertVectorToPrimitive;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.getVectorMagnitudeSquared;

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

        float queryVectorMagnitude = getVectorMagnitudeSquared(queryVector);
        float inputVectorMagnitude = getVectorMagnitudeSquared(inputVector);
        float dotProduct = 12.0f;
        float expectedScore = (float) (dotProduct / (Math.sqrt(queryVectorMagnitude * inputVectorMagnitude)));


        Float actualScore = KNNScoringUtil.cosinesimil(queryVector, inputVector);
        assertEquals(expectedScore, actualScore, 0.0001);
    }

    public void testCosineSimilOptimizedScoringFunction() {
        float[] queryVector = {1.0f, 1.0f, 1.0f};
        float[] inputVector = {4.0f, 4.0f, 4.0f};
        float queryVectorMagnitude = getVectorMagnitudeSquared(queryVector);
        float inputVectorMagnitude = getVectorMagnitudeSquared(inputVector);
        float dotProduct = 12.0f;
        float expectedScore = (float) (dotProduct / (Math.sqrt(queryVectorMagnitude * inputVectorMagnitude)));

        Float actualScore = KNNScoringUtil.cosinesimilOptimized(queryVector, inputVector, queryVectorMagnitude);
        assertEquals(expectedScore, actualScore, 0.0001);
    }

    public void testGetInvalidVectorMagnitudeSquared() {
        float[] queryVector = null;
        // vector cannot be null
        expectThrows(IllegalStateException.class, () -> getVectorMagnitudeSquared(queryVector));
    }

    public void testConvertInvalidVectorToPrimitive() {
        float[] primitiveVector = null;
        assertEquals(primitiveVector, convertVectorToPrimitive(primitiveVector));
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
        BigInteger bigInteger1 = new BigInteger("4", 16);
        BigInteger bigInteger2 = new BigInteger("32278", 16);
        BigInteger bigInteger3 = new BigInteger("AB5432", 16);
        BigInteger bigInteger4 = new BigInteger("EECCDDFF", 16);
        BigInteger bigInteger5 = new BigInteger("1114AB5432", 16);

        /*
         * Hex to binary table:
         *
         * 4            -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0100
         * 32278        -> 0000 0000 0000 0000 0000 0011 0010 0010 0111 1000
         * AB5432       -> 0000 0000 0000 0000 1010 1011 0101 0100 0011 0010
         * EECCDDFF     -> 0000 0000 1110 1110 1100 1100 1101 1101 1111 1111
         * 1114AB5432   -> 0001 0001 0001 0100 1010 1011 0101 0100 0011 0010
         */

        assertEquals(9.0, KNNScoringUtil.calculateHammingBit(bigInteger1, bigInteger2), 0.1);
        assertEquals(12.0, KNNScoringUtil.calculateHammingBit(bigInteger1, bigInteger3), 0.1);
        assertEquals(23.0, KNNScoringUtil.calculateHammingBit(bigInteger1, bigInteger4), 0.1);
        assertEquals(16.0, KNNScoringUtil.calculateHammingBit(bigInteger1, bigInteger5), 0.1);

        assertEquals(9.0, KNNScoringUtil.calculateHammingBit(bigInteger2, bigInteger1), 0.1);
        assertEquals(11.0, KNNScoringUtil.calculateHammingBit(bigInteger2, bigInteger3), 0.1);
        assertEquals(24.0, KNNScoringUtil.calculateHammingBit(bigInteger2, bigInteger4), 0.1);
        assertEquals(15.0, KNNScoringUtil.calculateHammingBit(bigInteger2, bigInteger5), 0.1);

        assertEquals(12.0, KNNScoringUtil.calculateHammingBit(bigInteger3, bigInteger1), 0.1);
        assertEquals(11.0, KNNScoringUtil.calculateHammingBit(bigInteger3, bigInteger2), 0.1);
        assertEquals(19.0, KNNScoringUtil.calculateHammingBit(bigInteger3, bigInteger4), 0.1);
        assertEquals(4.0, KNNScoringUtil.calculateHammingBit(bigInteger3, bigInteger5), 0.1);

        assertEquals(23.0, KNNScoringUtil.calculateHammingBit(bigInteger4, bigInteger1), 0.1);
        assertEquals(24.0, KNNScoringUtil.calculateHammingBit(bigInteger4, bigInteger2), 0.1);
        assertEquals(19.0, KNNScoringUtil.calculateHammingBit(bigInteger4, bigInteger3), 0.1);
        assertEquals(21.0, KNNScoringUtil.calculateHammingBit(bigInteger4, bigInteger5), 0.1);

        assertEquals(16.0, KNNScoringUtil.calculateHammingBit(bigInteger5, bigInteger1), 0.1);
        assertEquals(15.0, KNNScoringUtil.calculateHammingBit(bigInteger5, bigInteger2), 0.1);
        assertEquals(4.0, KNNScoringUtil.calculateHammingBit(bigInteger5, bigInteger3), 0.1);
        assertEquals(21.0, KNNScoringUtil.calculateHammingBit(bigInteger5, bigInteger4), 0.1);
    }

    public void testBitHammingDistance_Long() {
        Long long1 = 1_817L;
        Long long2 = 500_000_924_849_631L;
        Long long3 = -500_000_924_849_631L;

        /*
         * 64 bit 2's complement:
         * 1_817L                  -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0111 0001 1001
         * 500_000_924_849_631L    -> 0000 0000 0000 0001 1100 0110 1011 1111 1000 1001 1000 0011 0101 0101 1101 1111
         * -500_000_924_849_631L   -> 1111 1111 1111 1110 0011 1001 0100 0000 0111 0110 0111 1100 1010 1010 0010 0001
         */

        assertEquals(25.0, KNNScoringUtil.calculateHammingBit(long1, long2), 0.1);
        assertEquals(38.0, KNNScoringUtil.calculateHammingBit(long1, long3), 0.1);
        assertEquals(63.0, KNNScoringUtil.calculateHammingBit(long2, long3), 0.1);
        assertEquals(0.0, KNNScoringUtil.calculateHammingBit(long3, long3), 0.1);
    }
}
