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

import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class KNNScoringScriptFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private String similaritySpace;
    private String field;
    private Object query;
    private KNNScoringSpace knnScoringSpace;

    public KNNScoringScriptFactory(Map<String, Object> params, SearchLookup lookup) {
        KNNCounter.SCRIPT_QUERY_REQUESTS.increment();
        this.params = params;
        this.lookup = lookup;

        parseParameters();

        this.knnScoringSpace = KNNScoreSpaceFactory.getSpace(this.similaritySpace, this.query,
                lookup.doc().mapperService().fieldType(this.field));

    }

    private void parseParameters() {
        final Object field = params.get("field");
        if (field == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Missing parameter [field]");
        }

        this.field = field.toString();

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
        this.query = queryValue;
    }

    public boolean needs_score() {
        return false;
    }

    /**
     * For each segment, supply the KNNScoreScript that should be run on the values returned from the fetch phase.
     * Because the method to score the documents was set during Factory construction, the scripts are agnostic of
     * the similarity space. The KNNScoringSpace will return the correct script, given the query, the field type, and
     * the similarity space.
     *
     * @param ctx LeafReaderContext for the segment
     * @return ScoreScript to be executed
     */
    @SuppressWarnings("unchecked")
    @Override // called number of segments times
    public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
        return knnScoringSpace.getScoreScript(params, field, lookup, ctx);
    }
}
