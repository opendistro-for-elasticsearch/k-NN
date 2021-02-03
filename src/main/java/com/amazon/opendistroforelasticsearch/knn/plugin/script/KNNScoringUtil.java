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

import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorScriptDocValues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.lang.Math;
import java.util.List;
import java.util.Objects;

public class KNNScoringUtil {
    private static Logger logger = LogManager.getLogger(KNNScoringUtil.class);

    /**
     * checks both query vector and input vector has equal dimension
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @throws IllegalArgumentException if query vector and input vector has different dimensions
     */
    private static void requireEqualDimension(final float[] queryVector, final float[] inputVector) {
        Objects.requireNonNull(queryVector);
        Objects.requireNonNull(inputVector);
        if (queryVector.length != inputVector.length) {
            String errorMessage = String.format("query vector dimension mismatch. Expected: %d, Given: %d",
                    inputVector.length, queryVector.length);
            throw new IllegalArgumentException(errorMessage);
        }
    }



    /**
     * This method calculates L2 squared distance between query vector
     * and input vector
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return L2 score
     */
    public static float l2Squared(float[] queryVector, float[] inputVector) {
        requireEqualDimension(queryVector, inputVector);
        float squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i] - inputVector[i];
            squaredDistance += diff * diff;
        }
        return squaredDistance;
    }

    private static float[] toFloat(List<Number> inputVector) {
        Objects.requireNonNull(inputVector);
        float[] value = new float[inputVector.size()];
        int index = 0;
        for (final Number val : inputVector) {
            value[index++] = val.floatValue();
        }
        return value;
    }

    /**
     * Whitelisted l2Squared method for users to calculate L2 squared distance between query vector
     * and document vectors
     * Example
     *  "script": {
     *         "source": "1/(1 + l2Squared(params.query_vector, doc[params.field]))",
     *         "params": {
     *           "query_vector": [1, 2, 3.4],
     *           "field": "my_dense_vector"
     *         }
     *       }
     *
     * @param queryVector query vector
     * @param docValues   script doc values
     * @return L2 score
     */
    public static float l2Squared(List<Number> queryVector, KNNVectorScriptDocValues docValues) {
        return l2Squared(toFloat(queryVector), docValues.getValue());
    }

    /**
     * This method can be used script to avoid repeated calculation of normalization
     * for query vector for each filtered documents
     *
     * @param queryVector     query vector
     * @param inputVector     input vector
     * @param normQueryVector normalized query vector value.
     * @return cosine score
     */
    public static float cosinesimilOptimized(float[] queryVector, float[] inputVector, float normQueryVector) {
        requireEqualDimension(queryVector, inputVector);
        float dotProduct = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    /**
     * Whitelisted cosineSimilarity method that can be used in a script to avoid repeated
     * calculation of normalization for the query vector.
     * Example:
     *  "script": {
     *         "source": "cosineSimilarity(params.query_vector, docs[field], 1.0) ",
     *         "params": {
     *           "query_vector": [1, 2, 3.4],
     *           "field": "my_dense_vector"
     *         }
     *       }
     *
     * @param queryVector          query vector
     * @param docValues            script doc values
     * @param queryVectorMagnitude the magnitude of the query vector.
     * @return cosine score
     */
    public static float cosineSimilarity(
            List<Number> queryVector, KNNVectorScriptDocValues docValues, Number queryVectorMagnitude) {
        return cosinesimilOptimized(toFloat(queryVector), docValues.getValue(), queryVectorMagnitude.floatValue());
    }

    /**
     * This method calculates cosine similarity
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return cosine score
     */
    public static float cosinesimil(float[] queryVector, float[] inputVector) {
        requireEqualDimension(queryVector, inputVector);
        float dotProduct = 0.0f;
        float normQueryVector = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normQueryVector += queryVector[i] * queryVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    /**
     * Whitelisted cosineSimilarity method for users to calculate cosine similarity between query vectors and
     * document vectors
     * Example:
     *  "script": {
     *         "source": "cosineSimilarity(params.query_vector, docs[field]) ",
     *         "params": {
     *           "query_vector": [1, 2, 3.4],
     *           "field": "my_dense_vector"
     *         }
     *       }
     *
     * @param queryVector query vector
     * @param docValues   script doc values
     * @return cosine score
     */
    public static float cosineSimilarity(List<Number> queryVector, KNNVectorScriptDocValues docValues) {
        return cosinesimil(toFloat(queryVector), docValues.getValue());
    }


    /**
     * This method calculates hamming distance on 2 BigIntegers
     *
     * @param queryBigInteger BigInteger
     * @param inputBigInteger input BigInteger
     * @return hamming distance
     */
    public static float calculateHammingBit(BigInteger queryBigInteger, BigInteger inputBigInteger) {
        return inputBigInteger.xor(queryBigInteger).bitCount();
    }

    /**
     * This method calculates hamming distance on 2 longs
     *
     * @param queryLong query Long
     * @param inputLong input Long
     * @return hamming distance
     */
    public static float calculateHammingBit(Long queryLong, Long inputLong) {
        return Long.bitCount(queryLong ^ inputLong);
    }

    /**
     * This method calculates L1 distance between query vector
     * and input vector
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return L1 score
     */
    public static float l1Norm(float[] queryVector, float[] inputVector) {
        requireEqualDimension(queryVector, inputVector);
        float distance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i] - inputVector[i];
            distance += Math.abs(diff);
        }
        return distance;
    }

    /**
     * Whitelisted l1distance method for users to calculate L1 distance between query vector
     * and document vectors
     * Example
     *  "script": {
     *         "source": "1/(1 + l1Norm(params.query_vector, doc[params.field]))",
     *         "params": {
     *           "query_vector": [1, 2, 3.4],
     *           "field": "my_dense_vector"
     *         }
     *       }
     *
     * @param queryVector query vector
     * @param docValues   script doc values
     * @return L1 score
     */
    public static float l1Norm(List<Number> queryVector, KNNVectorScriptDocValues docValues) {
        return l1Norm(toFloat(queryVector), docValues.getValue());
    }
}
