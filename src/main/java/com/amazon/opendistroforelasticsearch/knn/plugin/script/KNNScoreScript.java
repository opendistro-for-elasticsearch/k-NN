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
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * KNNScoreScript is used for adjusting the score of query results based on similarity distance methods. Scripts
 * operate on a per document basis. Because the scoring method is passed in during construction, KNNScoreScripts are
 * only concerned with the types of the query and docs being processed.
 */
public abstract class KNNScoreScript<T> extends ScoreScript {
    protected final T queryValue;
    protected final String field;
    protected final BiFunction<T, T, Float> scoringMethod;

    public KNNScoreScript(Map<String, Object> params, T queryValue, String field,
                          BiFunction<T, T, Float> scoringMethod, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        this.queryValue = queryValue;
        this.field = field;
        this.scoringMethod = scoringMethod;
    }

    /**
     * KNNScoreScript with Long type. The query value passed in as well as the DocValues being searched over are
     * expected to be Longs.
     */
    public static class LongType extends KNNScoreScript<Long> {
        public LongType(Map<String, Object> params, Long queryValue, String field,
                        BiFunction<Long, Long, Float> scoringMethod, SearchLookup lookup,
                        LeafReaderContext leafContext) {
            super(params, queryValue, field, scoringMethod, lookup, leafContext);
        }

        /**
         * This function calculates the similarity score for each doc in the segment.
         *
         * @param explanationHolder A helper to take in an explanation from a script and turn
         *                          it into an {@link org.apache.lucene.search.Explanation}
         * @return score for the provided space between the doc and the query
         */
        @Override
        public double execute(ScoreScript.ExplanationHolder explanationHolder) {
            ScriptDocValues.Longs scriptDocValues = (ScriptDocValues.Longs) getDoc().get(this.field);
            if (scriptDocValues.isEmpty()) {
                return Float.MIN_VALUE;
            }
            return this.scoringMethod.apply(this.queryValue, scriptDocValues.getValue());
        }
    }

    /**
     * KNNScoreScript with BigInteger type. The query value passed in as well as the DocValues being searched over
     * are expected to be BigInteger.
     */
    public static class BigIntegerType extends KNNScoreScript<BigInteger> {
        public BigIntegerType(Map<String, Object> params, BigInteger queryValue, String field,
                              BiFunction<BigInteger, BigInteger, Float> scoringMethod, SearchLookup lookup,
                              LeafReaderContext leafContext) {
            super(params, queryValue, field, scoringMethod, lookup, leafContext);
        }

        /**
         * This function calculates the similarity score for each doc in the segment.
         *
         * @param explanationHolder A helper to take in an explanation from a script and turn
         *                          it into an {@link org.apache.lucene.search.Explanation}
         * @return score for the provided space between the doc and the query
         */
        @Override
        public double execute(ScoreScript.ExplanationHolder explanationHolder) {
            ScriptDocValues.BytesRefs scriptDocValues = (ScriptDocValues.BytesRefs) getDoc().get(this.field);
            if (scriptDocValues.isEmpty()) {
                return Float.MIN_VALUE;
            }
            return this.scoringMethod.apply(this.queryValue, new BigInteger(1, scriptDocValues.getValue().bytes));
        }
    }

    /**
     * KNNVectors with float[] type. The query value passed in is expected to be float[]. The fieldType of the docs
     * being searched over are expected to be KNNVector type.
     */
    public static class KNNVectorType extends KNNScoreScript<float[]> {

        public KNNVectorType(Map<String, Object> params, float[] queryValue, String field,
                             BiFunction<float[], float[], Float> scoringMethod, SearchLookup lookup,
                             LeafReaderContext leafContext) throws IOException {
            super(params, queryValue, field, scoringMethod, lookup, leafContext);
        }

        /**
         * This function called for each doc in the segment. We evaluate the score of the vector in the doc
         *
         * @param explanationHolder A helper to take in an explanation from a script and turn
         *                          it into an {@link org.apache.lucene.search.Explanation}
         * @return score of the vector to the query vector
         */
        @Override
        public double execute(ScoreScript.ExplanationHolder explanationHolder) {
            KNNVectorScriptDocValues scriptDocValues = (KNNVectorScriptDocValues) getDoc().get(this.field);
            if (scriptDocValues.isEmpty()) {
                return Float.MIN_VALUE;
            }
            return this.scoringMethod.apply(this.queryValue, scriptDocValues.getValue());
        }
    }
}
