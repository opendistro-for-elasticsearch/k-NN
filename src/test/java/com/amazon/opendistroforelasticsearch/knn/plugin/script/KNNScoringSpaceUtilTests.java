package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isBinaryFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isKNNVectorFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isLongFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToBigInteger;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToFloatArray;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNScoringSpaceUtilTests extends KNNTestCase {
    public void testFieldTypeCheck() {
        assertTrue(isLongFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.LONG)));
        assertFalse(isLongFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER)));
        assertFalse(isLongFieldType(new BinaryFieldMapper.BinaryFieldType("test")));

        assertTrue(isBinaryFieldType(new BinaryFieldMapper.BinaryFieldType("test")));
        assertFalse(isBinaryFieldType(new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.INTEGER)));

        assertTrue(isKNNVectorFieldType(mock(KNNVectorFieldMapper.KNNVectorFieldType.class)));
        assertFalse(isKNNVectorFieldType(new BinaryFieldMapper.BinaryFieldType("test")));
    }

    public void testParseLongQuery() {
        int integerQueryObject = 157;
        assertEquals(Long.valueOf(integerQueryObject), parseToLong(integerQueryObject));

        Long longQueryObject = 10001L;
        assertEquals(longQueryObject, parseToLong(longQueryObject));

        String invalidQueryObject = "invalid";
        expectThrows(IllegalArgumentException.class, () -> parseToLong(invalidQueryObject));
    }

    public void testParseBinaryQuery() {
        String base64String = "SrtFZw==";

        /*
         * B64:         "SrtFZw=="
         * Decoded Hex: 4ABB4567
         */

        assertEquals(new BigInteger("4ABB4567", 16), parseToBigInteger(base64String));
    }

    public void testParseKNNVectorQuery() {
        float[] arrayFloat = new float[]{1.0f, 2.0f, 3.0f};
        List<Double> arrayListQueryObject = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));

        KNNVectorFieldMapper.KNNVectorFieldType fieldType = mock(KNNVectorFieldMapper.KNNVectorFieldType.class);

        when(fieldType.getDimension()).thenReturn(3);
        assertArrayEquals(arrayFloat, parseToFloatArray(arrayListQueryObject, 3), 0.1f);

        expectThrows(IllegalStateException.class, () -> parseToFloatArray(arrayListQueryObject, 4));

        String invalidObject = "invalidObject";
        expectThrows(ClassCastException.class, () -> parseToFloatArray(invalidObject, 3));
    }
}
