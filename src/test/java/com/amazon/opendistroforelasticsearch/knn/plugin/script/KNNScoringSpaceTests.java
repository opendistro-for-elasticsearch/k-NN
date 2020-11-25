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
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static org.mockito.Mockito.mock;

public class KNNScoringSpaceTests extends KNNTestCase {

    public void testL2() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));
        KNNVectorFieldMapper.KNNVectorFieldType fieldType = new KNNVectorFieldMapper.KNNVectorFieldType("test",
                Collections.emptyMap(), 3);
        KNNScoringSpace.L2 l2 = new KNNScoringSpace.L2(arrayListQueryObject, fieldType);
        assertEquals(1F, l2.scoringMethod.apply(arrayFloat, arrayFloat), 0.1F);

        NumberFieldMapper.NumberFieldType invalidFieldType = new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.L2(arrayListQueryObject, invalidFieldType));
    }

    public void testCosineSimilarity() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));
        float[] arrayFloat2 = new float[]{2.0f, 4.0f, 6.0f};

        KNNVectorFieldMapper.KNNVectorFieldType fieldType = new KNNVectorFieldMapper.KNNVectorFieldType("test",
                Collections.emptyMap(), 3);
        KNNScoringSpace.CosineSimilarity cosineSimilarity =
                new KNNScoringSpace.CosineSimilarity(arrayListQueryObject, fieldType);

        assertEquals(3F, cosineSimilarity.scoringMethod.apply(arrayFloat2, arrayFloat), 0.1F);

        NumberFieldMapper.NumberFieldType invalidFieldType = new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.CosineSimilarity(arrayListQueryObject, invalidFieldType));
    }

    @SuppressWarnings("unchecked")
    public void testHammingBit_Long() {
        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.LONG);
        Long longObject1 = 1234L; // ..._0000_0100_1101_0010
        Long longObject2 = 2468L; // ..._0000_1001_1010_0100
        KNNScoringSpace.HammingBit hammingBit = new KNNScoringSpace.HammingBit(longObject1, fieldType);

        assertEquals(0.1111F,
                ((BiFunction<Long, Long, Float>)hammingBit.scoringMethod).apply(longObject1, longObject2), 0.1F);

        KNNVectorFieldMapper.KNNVectorFieldType invalidFieldType = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.HammingBit(longObject1, invalidFieldType));
    }

    @SuppressWarnings("unchecked")
    public void testHammingBit_Base64() {
        BinaryFieldMapper.BinaryFieldType fieldType = new BinaryFieldMapper.BinaryFieldType("field");
        String base64Object1 = "q83vQUI=";
        String base64Object2 = "//43ITI=";

        /*
         * Base64 to Binary
         * q83vQUI= -> 1010 1011 1100 1101 1110 1111 0100 0001 0100 0010
         * //43ITI= -> 1111 1111 1111 1110 0011 0111 0010 0001 0011 0010
         */

        float expectedResult = 1F / (1 + 16);
        KNNScoringSpace.HammingBit hammingBit = new KNNScoringSpace.HammingBit(base64Object1, fieldType);

        assertEquals(expectedResult,
                ((BiFunction<BigInteger, BigInteger, Float>)hammingBit.scoringMethod).apply(
                        new BigInteger(Base64.getDecoder().decode(base64Object1)),
                        new BigInteger(Base64.getDecoder().decode(base64Object2))
                ), 0.1F);

        KNNVectorFieldMapper.KNNVectorFieldType invalidFieldType = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.HammingBit(base64Object1, invalidFieldType));
    }
}
