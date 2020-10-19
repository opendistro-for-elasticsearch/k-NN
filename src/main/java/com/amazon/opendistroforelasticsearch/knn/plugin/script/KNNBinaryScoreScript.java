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

import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.BitSet;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Binary score script used for adjusting the score based on binary similarity spaces
 * on a per document basis.
 *
 */
public class KNNBinaryScoreScript extends ScoreScript {
    private final BitSet queryBitSet;
    private final String similaritySpace;
    private final String field;
    private final BiFunction<BitSet, BitSet, Float> distanceMethod;

    /**
     * This function calculates the bit hamming score for each doc in the segment.
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
        return 1/(1 + this.distanceMethod.apply(this.queryBitSet,
                BitSet.valueOf(((BytesRef) scriptDocValues.get(0)).bytes)));
    }

    public KNNBinaryScoreScript(Map<String, Object> params, String field, BitSet queryBitSet, String similaritySpace,
                                SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        this.similaritySpace = similaritySpace;
        this.queryBitSet = queryBitSet;
        this.field = field;

        if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {
            this.distanceMethod = KNNScoringUtil::bitHamming;
        } else {
            throw new IllegalArgumentException("Invalid space type for KNNBinaryScoreScript: " + similaritySpace);
        }
    }
}
