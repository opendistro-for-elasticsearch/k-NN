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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * KNNScoreScript is used for adjusting the score based on a similarity distance method on a per document basis.
 *
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
     * A script score that takes a list of long query and Long doc values and calculates the distance between them
     * based on the distance function passed into the constructor
     */
    public static class KNNLongListScoreScript extends KNNScoreScript<List<Long>> {
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
            if (scriptDocValues.size() == 0) {
                return Float.MIN_VALUE;
            }
            return this.scoringMethod.apply(this.queryValue, scriptDocValues.subList(0, scriptDocValues.size()));
        }

        public KNNLongListScoreScript(Map<String, Object> params, String field, List<Long> queryValue,
                                  BiFunction<List<Long>, List<Long>, Float> scoringMethod, SearchLookup lookup,
                                  LeafReaderContext leafContext) {
            super(params, queryValue, field, scoringMethod, lookup, leafContext);
        }
    }
}
