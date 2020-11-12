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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNScoringSpaceTests extends KNNTestCase {
    public void testFieldTypeCheck() {
        TestScoringSpace testScoringSpace = new TestScoringSpace(null, null);

        assertTrue(testScoringSpace.isLongFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.LONG)));
        assertFalse(testScoringSpace.isLongFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER)));
        assertFalse(testScoringSpace.isLongFieldType(new BinaryFieldMapper.BinaryFieldType("test")));

        assertTrue(testScoringSpace.isBinaryFieldType(new BinaryFieldMapper.BinaryFieldType("test")));
        assertFalse(testScoringSpace.isBinaryFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER)));

        assertTrue(testScoringSpace.isKNNVectorFieldType(mock(KNNVectorFieldMapper.KNNVectorFieldType.class)));
        assertFalse(testScoringSpace.isKNNVectorFieldType(new BinaryFieldMapper.BinaryFieldType("test")));
    }

    public void testParseLongQuery() {
        TestScoringSpace testScoringSpace = new TestScoringSpace(null, null);
        int integerQueryObject = 157;
        assertEquals(Long.valueOf(integerQueryObject), testScoringSpace.parseLongQuery(integerQueryObject));

        Long longQueryObject = 10001L;
        assertEquals(longQueryObject, testScoringSpace.parseLongQuery(longQueryObject));

        String invalidQueryObject = "invalid";
        expectThrows(IllegalArgumentException.class, () -> testScoringSpace.parseLongQuery(invalidQueryObject));
    }

    public void testParseBinaryQuery() {
        TestScoringSpace testScoringSpace = new TestScoringSpace(null, null);

        // Hex: FF FF FF FF FF FF FF FF
        // Binary: 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
        String base64Encoding = "//////////8=";
        BitSet bitSet = new BitSet(64);
        bitSet.set(0, 64);

        assertEquals(bitSet, testScoringSpace.parseBinaryQuery(base64Encoding));

        String invalidBase64String = "invalidBase64~~~~";
        expectThrows(IllegalArgumentException.class, () -> testScoringSpace.parseLongQuery(invalidBase64String));
    }

    public void testParseKNNVectorQuery() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));

        KNNVectorFieldMapper.KNNVectorFieldType fieldType = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        when(fieldType.getDimension()).thenReturn(3);
        TestScoringSpace testScoringSpace = new TestScoringSpace(null, fieldType);

        assertArrayEquals(arrayFloat, testScoringSpace.parseKNNVectorQuery(arrayListQueryObject), 0.1f);

        when(fieldType.getDimension()).thenReturn(4);
        expectThrows(IllegalStateException.class, () -> testScoringSpace.parseKNNVectorQuery(arrayListQueryObject));

        String invalidObject = "invalidObject";
        expectThrows(ClassCastException.class, () -> testScoringSpace.parseKNNVectorQuery(invalidObject));
    }

    @SuppressWarnings("unchecked")
    public void testL2() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));
        KNNVectorFieldMapper.KNNVectorFieldType fieldType = new KNNVectorFieldMapper.KNNVectorFieldType("test",
                Collections.emptyMap(), 3);
        KNNScoringSpace.L2 l2 = new KNNScoringSpace.L2(arrayListQueryObject, fieldType);
        assertEquals(1F, ((BiFunction<float[], float[], Float>)l2.scoringMethod).apply(arrayFloat, arrayFloat),
                0.1F);

        NumberFieldMapper.NumberFieldType invalidFieldType = new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.L2(arrayListQueryObject, invalidFieldType));
    }

    @SuppressWarnings("unchecked")
    public void testCosineSimilarity() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));
        float[] arrayFloat2 = new float[]{2.0f, 4.0f, 6.0f};

        KNNVectorFieldMapper.KNNVectorFieldType fieldType = new KNNVectorFieldMapper.KNNVectorFieldType("test",
                Collections.emptyMap(), 3);
        KNNScoringSpace.CosineSimilarity cosineSimilarity =
                new KNNScoringSpace.CosineSimilarity(arrayListQueryObject, fieldType);

        assertEquals(3F,
                ((BiFunction<float[], float[], Float>)cosineSimilarity.scoringMethod).apply(arrayFloat2, arrayFloat),
                0.1F);

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
        String base64Object1 = "q83vQUI="; // AB_CD_EF_41_42 1010_1011_1100_1101_1110_1111_0100_0001_0100_0010
        String base64Object2 = "//43ITI="; // FF_FE_37_21_32 1111_1111_1111_1110_0011_0111_0010_0001_0011_0010
        float expectedResult = 1F / (1 + 16);
        KNNScoringSpace.HammingBit hammingBit = new KNNScoringSpace.HammingBit(base64Object1, fieldType);

        assertEquals(expectedResult,
                ((BiFunction<BitSet, BitSet, Float>)hammingBit.scoringMethod).apply(
                        BitSet.valueOf(Base64.getDecoder().decode(base64Object1)),
                        BitSet.valueOf(Base64.getDecoder().decode(base64Object2))
                ), 0.1F);

        KNNVectorFieldMapper.KNNVectorFieldType invalidFieldType = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        expectThrows(IllegalArgumentException.class, () ->
                new KNNScoringSpace.HammingBit(base64Object1, invalidFieldType));
    }

    public static class TestScoringSpace extends KNNScoringSpace {
        public TestScoringSpace(Object query, MappedFieldType fieldType) {
            super(query, fieldType);
        }

        @Override
        public void prepareQuery(Object query) {

        }

        @Override
        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup,
                                          LeafReaderContext ctx) throws IOException {
            return null;
        }
    }
}
