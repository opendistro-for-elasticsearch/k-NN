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

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.elasticsearch.index.mapper.NumberFieldMapper;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class KNNScoringSpaceFactoryTests extends KNNTestCase {
    public void testValidSpaces() {

        KNNVectorFieldMapper.KNNVectorFieldType knnVectorFieldType =
                mock(KNNVectorFieldMapper.KNNVectorFieldType.class);
        NumberFieldMapper.NumberFieldType numberFieldType = new NumberFieldMapper.NumberFieldType("field",
                NumberFieldMapper.NumberType.LONG);
        List<Float> floatQueryObject = new ArrayList<>();
        Long longQueryObject = 0L;

        assertTrue(KNNScoringSpaceFactory.create(KNNConstants.L2, floatQueryObject, knnVectorFieldType)
                instanceof KNNScoringSpace.L2);
        assertTrue(KNNScoringSpaceFactory.create(KNNConstants.COSINESIMIL, floatQueryObject, knnVectorFieldType)
                instanceof KNNScoringSpace.CosineSimilarity);
        assertTrue(KNNScoringSpaceFactory.create(KNNConstants.HAMMING_BIT, longQueryObject, numberFieldType)
                instanceof KNNScoringSpace.HammingBit);
    }

    public void testInvalidSpace() {
        expectThrows(IllegalArgumentException.class, () -> KNNScoringSpaceFactory.create(KNNConstants.L2,
                null, null));
    }
}
