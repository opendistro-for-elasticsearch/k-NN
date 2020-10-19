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
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * KNNScoreScript is used for adjusting the score based on a similarity distance method on a per document basis.
 *
 */
public abstract class KNNScoreScript<T> extends ScoreScript {
    protected final T queryValue;
    protected final String field;
    protected final BiFunction<T, T, Float> distanceMethod;

    public KNNScoreScript(Map<String, Object> params, T queryValue, String field,
                          BiFunction<T, T, Float> distanceMethod, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        this.queryValue = queryValue;
        this.field = field;
        this.distanceMethod = distanceMethod;
    }
}
