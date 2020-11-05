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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.LONG;

public class KNNScoringScriptFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private String similaritySpace;
    private String field;

    private Object qValue;
    private BiFunction<?, ?, Float> scoringMethod;

    private MappedFieldType fieldType;

    public KNNScoringScriptFactory(Map<String, Object> params, SearchLookup lookup) {
        KNNCounter.SCRIPT_QUERY_REQUESTS.increment();
        this.params = params;
        this.lookup = lookup;

        parseParameters();

        this.fieldType = lookup.doc().mapperService().fieldType(this.field);

        configureQuery();
    }


    private void parseParameters() {
        // Confirm query passed a field
        final Object field = params.get("field");
        if (field == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [field]");
        }

        this.field = field.toString();

        // Confirm query passed space_type parameter
        final Object space = params.get("space_type");
        if (space == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [space_type]");
        }

        this.similaritySpace = space.toString();

        final Object queryValue = params.get("query_value");
        if (queryValue == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [query_value]");
        }
        this.qValue = queryValue;
    }

    /**
     * A function to properly configure the script query before passing it to the script. To do this, the query_value
     * passed in must be cast to the correct type for the given space and the scoring method for the
     * datatype/similarity space combination should be set accordingly.
     */
    @SuppressWarnings("unchecked")
    private void configureQuery() {
        if (KNNConstants.L2.equalsIgnoreCase(similaritySpace)) {
            this.qValue = KNNScoringUtil.convertVectorToPrimitive(this.qValue);
            this.scoringMethod = (float[] q, float[] v) -> 1/(1 + KNNScoringUtil.l2Squared(q, v));
        } else if (KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace)) {
            this.qValue = KNNScoringUtil.convertVectorToPrimitive(qValue);
            float qVectorSquaredMagnitude = KNNScoringUtil.getVectorMagnitudeSquared((float[]) this.qValue);
            this.scoringMethod = (float[] q, float[] v) -> 1 + KNNScoringUtil.cosinesimilOptimized(q, v,
                    qVectorSquaredMagnitude);
        } else if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {

            if (!(fieldType instanceof NumberFieldMapper.NumberFieldType)) {
                throw new IllegalArgumentException("Incompatible field_type for hamming space. The field type must " +
                        "be an integral numeric type.");
            }

            if (((NumberFieldMapper.NumberFieldType) fieldType).numericType() == LONG.numericType()) {
                // Make sure the query is a list of longs
                if (qValue instanceof Integer) {
                    qValue = Collections.singletonList(Long.valueOf((Integer) qValue));
                } else if (qValue instanceof Long) {
                    qValue = Collections.singletonList((Long) qValue);
                } else if (qValue instanceof List && ((List<?>) this.qValue).iterator().next() instanceof Integer) {
                    qValue = ((List<Integer>) this.qValue).stream().mapToLong(Integer::longValue).boxed()
                            .collect(Collectors.toList());
                } else if (!(qValue instanceof List) ||
                        (((List<?>) this.qValue).size() != 0 &&
                                !(((List<?>) this.qValue).iterator().next() instanceof Long))
                ) {
                    throw new IllegalArgumentException("Incompatible query_value for hamming space. query_value must " +
                            "be either a Long, an Integer, an array of Longs, or an array of Integers.");
                }
                // Need to reverse the list because Elasticsearch stores lists in reverse order
                // Because this happens once per query, this does not incur a major latency penalty
                this.qValue = new ArrayList<>((List<Long>) this.qValue);
                Collections.reverse((List<Long>) this.qValue);
                this.scoringMethod = (List<Long> q, List<Long> v) -> 1.0f/(1 + KNNScoringUtil.bitHamming(q, v));
            } else {
                throw new IllegalArgumentException("Incompatible field_type for hamming space. The field type must " +
                        "of type Long.");
            }
        } else {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Invalid space type. Please refer to the available space types.");
        }
    }

    public boolean needs_score() {
        return false;
    }

    /**
     * For each segment, supply the KNNScoreScript that should be run on the values returned from the fetch phase.
     * Because the method to score the documents was set during Factory construction, the scripts are agnostic of
     * the similarity space. The qValue was set during factory construction as well. This will determine which score
     * script needs to be called.
     *
     * @param ctx LeafReaderContext for the segment
     * @return ScoreScript to be executed
     * @throws IOException can be thrown during construction of ScoreScript
     */
    @SuppressWarnings("unchecked")
    @Override // called number of segments times
    public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {

        if (this.qValue instanceof float[]) {
            return new KNNVectorScoreScript(this.params, this.field, (float[]) this.qValue,
                    (BiFunction<float[], float[], Float>) this.scoringMethod, this.lookup, ctx);
        } else if (this.qValue instanceof List && ((List<?>) this.qValue).iterator().next() instanceof Long) {
            return new KNNScoreScript.KNNLongListScoreScript(this.params, this.field, (List<Long>) this.qValue,
                    (BiFunction<List<Long>, List<Long>, Float>) this.scoringMethod, this.lookup, ctx);
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
