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
import org.elasticsearch.index.mapper.MappedFieldType;

public class KNNScoreSpaceFactory {
    public static KNNScoringSpace getSpace(String spaceType, Object query, MappedFieldType mappedFieldType) {
        if (spaceType.equalsIgnoreCase(KNNConstants.BIT_HAMMING)) {
            return new KNNScoringSpace.HammingBitSpace(query, mappedFieldType);
        } else if (spaceType.equalsIgnoreCase(KNNConstants.L2)) {
            return new KNNScoringSpace.L2Space(query, mappedFieldType);
        } else if (spaceType.equalsIgnoreCase(KNNConstants.COSINESIMIL)) {
            return new KNNScoringSpace.CosineSimilaritySpace(query, mappedFieldType);
        } else {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalArgumentException("Invalid space type. Please refer to the available space types.");
        }
    }
}
