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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.BitSet;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * KNNBitSetScoreScript is used for adjusting the score based on binary similarity spaces on a per document basis.
 *
 */
public class KNNBitSetScoreScript extends KNNScoreScript<BitSet> {

    /**
     * This function calculates the score for each doc in the segment based on the distance method passed into the
     * constructor
     *
     * @param explanationHolder A helper to take in an explanation from a script and turn
     *                          it into an {@link org.apache.lucene.search.Explanation}
     * @return score for the provided space between the doc and the query
     */
    @Override
    public double execute(ScoreScript.ExplanationHolder explanationHolder) {
        ScriptDocValues<?> scriptDocValues = getDoc().get(this.field);
        if (scriptDocValues.size() == 0) {
            return Float.MIN_VALUE;
        }
        return 1/(1 + this.distanceMethod.apply(this.queryValue,
                BitSet.valueOf(((BytesRef) scriptDocValues.get(0)).bytes)));
    }

    public KNNBitSetScoreScript(Map<String, Object> params, String field, BitSet queryValue,
                              BiFunction<BitSet, BitSet, Float> distanceMethod, SearchLookup lookup,
                              LeafReaderContext leafContext) {
        super(params, queryValue, field, distanceMethod, lookup, leafContext);
    }
}
