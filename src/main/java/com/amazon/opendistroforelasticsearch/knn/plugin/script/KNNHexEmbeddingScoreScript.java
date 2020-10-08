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
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.math.BigInteger;
import java.util.Map;

/**
 * Binary score script used for adjusting the score based on binary similarity spaces
 * on a per document basis.
 *
 */
public class KNNHexEmbeddingScoreScript extends ScoreScript {

    private final BigInteger queryHexEmbedding;
    private final String similaritySpace;
    private final String field;

    /**
     * This function calculates the custom score for each doc in the segment.
     *
     * @param explanationHolder A helper to take in an explanation from a script and turn
     *                          it into an {@link org.apache.lucene.search.Explanation}
     * @return score for the provided space between the doc and the query
     */
    @Override
    public double execute(ScoreScript.ExplanationHolder explanationHolder) {
        float score = Float.MIN_VALUE;
        if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {
            BigInteger docHash = new BigInteger(getDoc().get(this.field).get(0).toString(), 16);
            score = 1/(1 + KNNScoringUtil.hamming(this.queryHexEmbedding, docHash));
        }

        return score;
    }

    @SuppressWarnings("unchecked")
    public KNNHexEmbeddingScoreScript(Map<String, Object> params, String field, BigInteger queryHexEmbedding,
                                String similaritySpace, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        this.similaritySpace = similaritySpace;
        this.queryHexEmbedding = queryHexEmbedding;
        this.field = field;
    }
}
