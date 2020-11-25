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

import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;

import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.LONG;

public class KNNScoringSpaceUtil {

    /**
     * Check if the passed in fieldType is of type NumberFieldType with numericType being Long
     *
     * @param fieldType MappedFieldType
     * @return true if fieldType is of type NumberFieldType and its numericType is Long; false otherwise
     */
    public static boolean isLongFieldType(MappedFieldType fieldType) {
        return fieldType instanceof NumberFieldMapper.NumberFieldType
                && ((NumberFieldMapper.NumberFieldType) fieldType).numericType() == LONG.numericType();
    }

    /**
     * Check if the passed in fieldType is of type BinaryFieldType
     *
     * @param fieldType MappedFieldType
     * @return true if fieldType is of type BinaryFieldType; false otherwise
     */
    public static boolean isBinaryFieldType(MappedFieldType fieldType) {
        return fieldType instanceof BinaryFieldMapper.BinaryFieldType;
    }

    /**
     * Check if the passed in fieldType is of type KNNVectorFieldType
     *
     * @param fieldType MappedFieldType
     * @return true if fieldType is of type KNNVectorFieldType; false otherwise
     */
    public static boolean isKNNVectorFieldType(MappedFieldType fieldType) {
        return fieldType instanceof KNNVectorFieldMapper.KNNVectorFieldType;
    }

    /**
     * Convert an Object to a Long.
     *
     * @param object Object to be parsed to a Long
     * @return Long of the object
     */
    public static Long parseToLong(Object object) {
        if (object instanceof Integer) {
            return Long.valueOf((Integer) object);
        }

        if (object instanceof Long) {
            return (Long) object;
        }

        throw new IllegalArgumentException("Object cannot be parsed as a Long.");
    }

    /**
     * Convert an Object to a BigInteger.
     *
     * @param object Base64 encoded String
     * @return BigInteger containing the bytes of decoded object
     */
    public static BigInteger parseToBigInteger(Object object) {
        return new BigInteger(1, Base64.getDecoder().decode((String) object));
    }

    /**
     * Convert an Object to a float array.
     *
     * @param object Object to be converted to a float array
     * @param expectedDimensions int representing the expected dimension of this array.
     * @return float[] of the object
     */
    public static float[] parseToFloatArray(Object object, int expectedDimensions) {
        float[] floatArray = convertVectorToPrimitive(object);
        if (expectedDimensions != floatArray.length) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalStateException("Object's dimension=" + floatArray.length + " does not match the " +
                    "expected dimension=" + expectedDimensions + ".");
        }
        return floatArray;
    }

    /**
     * Converts Object vector to primitive float[]
     *
     * @param vector input vector
     * @return Float array representing the vector
     */
    @SuppressWarnings("unchecked")
    public static float[] convertVectorToPrimitive(Object vector) {
        float[] primitiveVector = null;
        if (vector != null) {
            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
            primitiveVector = new float[tmp.size()];
            for (int i = 0; i < primitiveVector.length; i++) {
                primitiveVector[i] = tmp.get(i).floatValue();
            }
        }
        return primitiveVector;
    }

    /**
     * Calculates the magnitude of given vector
     *
     * @param inputVector input vector
     * @return Magnitude of vector
     */
    public static float getVectorMagnitudeSquared(float[] inputVector) {
        if (null == inputVector) {
            throw new IllegalStateException("vector magnitude cannot be evaluated as it is null");
        }
        float normInputVector = 0.0f;
        for (int i = 0; i < inputVector.length; i++) {
            normInputVector += inputVector[i] * inputVector[i];
        }
        return normInputVector;
    }
}
