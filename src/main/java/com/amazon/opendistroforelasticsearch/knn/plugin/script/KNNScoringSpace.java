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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.function.BiFunction;

import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.getVectorMagnitudeSquared;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isBinaryFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isKNNVectorFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.isLongFieldType;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToBigInteger;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToFloatArray;
import static com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringSpaceUtil.parseToLong;


public interface KNNScoringSpace {
    /**
     * Return the correct scoring script for a given query. The scoring script
     *
     * @param params Map of parameters
     * @param field Fieldname
     * @param lookup SearchLookup
     * @param ctx ctx LeafReaderContext to be used for scoring documents
     * @return ScoreScript for this query
     * @throws IOException throws IOException if ScoreScript cannot be constructed
     */
    ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup, LeafReaderContext ctx)
            throws IOException;

    class L2 implements KNNScoringSpace {

        float[] processedQuery;
        BiFunction<float[], float[], Float> scoringMethod;

        /**
         * Constructor for L2 scoring space. L2 scoring space expects values to be of type float[].
         *
         * @param query Query object that, along with the doc values, will be used to compute L2 score
         * @param fieldType FieldType for the doc values that will be used
         */
        public L2(Object query, MappedFieldType fieldType) {
            if (!isKNNVectorFieldType(fieldType)) {
                throw new IllegalArgumentException("Incompatible field_type for l2 space. The field type must " +
                        "be knn_vector.");
            }

            this.processedQuery = parseToFloatArray(query,
                    ((KNNVectorFieldMapper.KNNVectorFieldType) fieldType).getDimension());
            this.scoringMethod = (float[] q, float[] v) -> 1 / (1 + KNNScoringUtil.l2Squared(q, v));
        }

        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup,
                                          LeafReaderContext ctx) throws IOException {
            return new KNNScoreScript.KNNVectorType(params, this.processedQuery, field, this.scoringMethod, lookup,
                    ctx);
        }
    }

    class CosineSimilarity implements KNNScoringSpace {

        float[] processedQuery;
        BiFunction<float[], float[], Float> scoringMethod;

        /**
         * Constructor for CosineSimilarity scoring space. CosineSimilarity scoring space expects values to be of type
         * float[].
         *
         * @param query Query object that, along with the doc values, will be used to compute CosineSimilarity score
         * @param fieldType FieldType for the doc values that will be used
         */
        public CosineSimilarity(Object query, MappedFieldType fieldType) {
            if (!isKNNVectorFieldType(fieldType)) {
                throw new IllegalArgumentException("Incompatible field_type for cosine space. The field type must " +
                        "be knn_vector.");
            }

            this.processedQuery = parseToFloatArray(query,
                    ((KNNVectorFieldMapper.KNNVectorFieldType) fieldType).getDimension());
            float qVectorSquaredMagnitude = getVectorMagnitudeSquared(this.processedQuery);
            this.scoringMethod = (float[] q, float[] v) -> 1 + KNNScoringUtil.cosinesimilOptimized(q, v,
                    qVectorSquaredMagnitude);
        }

        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup,
                                          LeafReaderContext ctx) throws IOException {
                return new KNNScoreScript.KNNVectorType(params, this.processedQuery, field, this.scoringMethod, lookup,
                        ctx);
        }
    }

    class HammingBit implements KNNScoringSpace {

        Object processedQuery;
        BiFunction<?, ?, Float> scoringMethod;

        /**
         * Constructor for HammingBit scoring space. HammingBit scoring space expects values to either be of type
         * Long or Base64 encoded strings.
         *
         * @param query Query object that, along with the doc values, will be used to compute HammingBit score
         * @param fieldType FieldType for the doc values that will be used
         */
        public HammingBit(Object query, MappedFieldType fieldType) {
            if (isLongFieldType(fieldType)) {
                this.processedQuery = parseToLong(query);
                this.scoringMethod = (Long q, Long v) -> 1.0f / (1 + KNNScoringUtil.calculateHammingBit(q, v));
            } else if (isBinaryFieldType(fieldType)) {
                this.processedQuery = parseToBigInteger(query);
                this.scoringMethod = (BigInteger q, BigInteger v) ->
                        1.0f / (1 + KNNScoringUtil.calculateHammingBit(q, v));
            } else {
                throw new IllegalArgumentException("Incompatible field_type for hamming space. The field type must " +
                        "of type long or binary.");
            }
        }

        @SuppressWarnings("unchecked")
        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup,
                                          LeafReaderContext ctx) throws IOException {
            if (this.processedQuery instanceof Long) {
                return new KNNScoreScript.LongType(params, (Long) this.processedQuery, field,
                        (BiFunction<Long, Long, Float>) this.scoringMethod, lookup, ctx);
            }

            return new KNNScoreScript.BigIntegerType(params, (BigInteger) this.processedQuery, field,
                    (BiFunction<BigInteger, BigInteger, Float>) this.scoringMethod, lookup, ctx);
        }
    }

    class L1 implements KNNScoringSpace {

        float[] processedQuery;
        BiFunction<float[], float[], Float> scoringMethod;

        /**
         * Constructor for L1 scoring space. L1 scoring space expects values to be of type float[].
         *
         * @param query Query object that, along with the doc values, will be used to compute L1 score
         * @param fieldType FieldType for the doc values that will be used
         */
        public L1(Object query, MappedFieldType fieldType) {
            if (!isKNNVectorFieldType(fieldType)) {
                throw new IllegalArgumentException("Incompatible field_type for l1 space. The field type must " +
                        "be knn_vector.");
            }

            this.processedQuery = parseToFloatArray(query,
                    ((KNNVectorFieldMapper.KNNVectorFieldType) fieldType).getDimension());
            this.scoringMethod = (float[] q, float[] v) -> 1 / (1 + KNNScoringUtil.l1Norm(q, v));
        }

        public ScoreScript getScoreScript(Map<String, Object> params, String field, SearchLookup lookup,
                                          LeafReaderContext ctx) throws IOException {
            return new KNNScoreScript.KNNVectorType(params, this.processedQuery, field, this.scoringMethod, lookup,
                    ctx);
        }
    }
}
