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
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

public class VectorScoreScriptFactory implements ScoreScript.LeafFactory {

    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private String similaritySpace;
    private String field;

    private float[] qVector;
    private float qVectorSquaredMagnitude;  // Used for cosine optimization
    private BigInteger qHash;

    public VectorScoreScriptFactory(Map<String, Object> params, SearchLookup lookup) {
        KNNCounter.SCRIPT_QUERY_REQUESTS.increment();
        this.params = params;
        this.lookup = lookup;
        validateAndInitParams(params);
    }

    private void validateAndInitParams(Map<String, Object> params) {
        // query vector field
        final Object field = params.get("field");
        if (field == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [field]");
        }

        this.field = field.toString();

        // validate space
        final Object space = params.get("space_type");
        if (space == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [space_type]");
        }

        this.similaritySpace = (String)space;

        if (KNNConstants.L2.equalsIgnoreCase(similaritySpace)
                || KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace)) {
            Object queryObject = params.get("vector");
            if (queryObject == null) {
                KNNCounter.SCRIPT_QUERY_ERRORS.increment();
                throw new IllegalArgumentException("Missing query vector parameter [vector]");
            }
            this.qVector = KNNScoringUtil.convertVectorToPrimitive(queryObject);
            if (KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace)) {
                qVectorSquaredMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(qVector);
            }
        } else if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {
            Object queryObject = params.get("hex_embedding");
            if (queryObject == null) {
                KNNCounter.SCRIPT_QUERY_ERRORS.increment();
                throw new IllegalArgumentException("Missing query binary parameter [hex_embedding]");
            }
            this.qHash = new BigInteger((String) queryObject, 16);
        } else {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Invalid space type. Please refer to the available space types.");
        }
    }

    public boolean needs_score() {
        return false;
    }

    @Override // called number of segments times
    public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
        if (ctx.reader().getBinaryDocValues(this.field) != null && (KNNConstants.L2.equalsIgnoreCase(similaritySpace)
                || KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace))) {
            return new KNNVectorScoreScript(this.params, this.field, this.qVector, this.qVectorSquaredMagnitude,
                    this.similaritySpace, this.lookup, ctx);
        } else if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {
            return new KNNHexEmbeddingScoreScript(this.params, this.field, this.qHash, this.similaritySpace,
                    this.lookup, ctx);
        }

        /*
         * the field and/or term don't exist in this segment,
         * so always return 0
         */
        return new ScoreScript(params, lookup, ctx) {
            @Override
            public double execute(
                    ExplanationHolder explanation
            ) {
                return 0.0d;
            }
        };
    }
}
