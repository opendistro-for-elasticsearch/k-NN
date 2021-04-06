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

import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.elasticsearch.index.mapper.MappedFieldType;

/**
 * Factory to create correct KNNScoringSpace based on the spaceType passed in.
 */
public class KNNScoringSpaceFactory {
    public static KNNScoringSpace create(String spaceType, Object query, MappedFieldType mappedFieldType) {
        if (SpaceType.HAMMING_BIT.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.HammingBit(query, mappedFieldType);
        }

        if (SpaceType.L2.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.L2(query, mappedFieldType);
        }

        if (SpaceType.L1.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.L1(query, mappedFieldType);
        }

        if (SpaceType.LINF.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.LInf(query, mappedFieldType);
        }

        if (SpaceType.INNER_PRODUCT.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.InnerProd(query, mappedFieldType);
        }

        if (SpaceType.COSINESIMIL.getValue().equalsIgnoreCase(spaceType)) {
            return new KNNScoringSpace.CosineSimilarity(query, mappedFieldType);
        }

        KNNCounter.SCRIPT_QUERY_ERRORS.increment();
        throw new IllegalArgumentException("Invalid space type. Please refer to the available space types.");
    }
}
