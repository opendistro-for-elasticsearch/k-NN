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
import java.util.Base64;
import java.util.BitSet;
import java.util.Map;

import static org.mockito.Mockito.mock;

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
        String base64String = "SrtFZw==";

        // Hex: 4ABB4567
        // Binary: 0100 1010 1011 1011 0100 0101 0110 0111
        BitSet bitSet = new BitSet(32);
        bitSet.set(30); bitSet.set(27); bitSet.set(25); bitSet.set(23); bitSet.set(21); bitSet.set(20); bitSet.set(19);
        bitSet.set(17); bitSet.set(16); bitSet.set(14); bitSet.set(10); bitSet.set(8); bitSet.set(6); bitSet.set(5);
        bitSet.set(2); bitSet.set(1); bitSet.set(0);


        // From online, for Hex representation(4ABB4567), SrtFZw==
        logger.info("Bitset returned by function: " + BitSet.valueOf(Base64.getDecoder().decode(base64String)));
        logger.info("Encoding produced using test bitSet: " + Base64.getEncoder().encodeToString(bitSet.toByteArray()));

        assertEquals(bitSet, testScoringSpace.parseBinaryQuery(base64String));
    }

    public void testParseKNNVectorQuery() {}

    public void testHammingBit() {}

    public void testL2() {}

    public void testCosine() {}

    public static class TestScoringSpace extends KNNScoringSpace {


        public TestScoringSpace(Object query, MappedFieldType fieldType) {
            super(query, fieldType);
        }

        @Override
        public void prepareQuery(Object query) {

        }

        @Override
        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup, LeafReaderContext ctx) throws IOException {
            return null;
        }
    }

}