/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.index;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum contains spaces supported for approximate nearest neighbor search in the k-NN plugin. Each engine's methods are
 * expected to support a subset of these spaces. Validation should be done in the jni layer and an exception should be
 * propagated up to the Java layer. Additionally, naming translations should be done in jni layer as well. For example,
 * nmslib calls the inner_product space "negdotprod". This translation should take place in the nmslib's jni layer.
 */
public enum SpaceType {
    L2("l2") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    },
    COSINESIMIL("cosinesimil") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    },
    L1("l1") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    },
    LINF("linf") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    },
    INNER_PRODUCT("innerproduct") {
        /**
         * The inner product has a range of [-Float.MAX_VALUE, Float.MAX_VALUE], with a more similar result being
         * represented by a more negative value. In Lucene, scores have to be in the range of [0, Float.MAX_VALUE],
         * where a higher score represents a more similar result. So, we convert here.
         *
         * @param rawScore score returned from underlying library
         * @return Lucene scaled score
         */
        @Override
        public float scoreTranslation(float rawScore) {
            if (rawScore >= 0) {
                return 1 / (1 + rawScore);
            }
            return -rawScore + 1;
        }
    },
    HAMMING_BIT("hammingbit") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    };

    public static SpaceType DEFAULT = L2;

    private final String value;

    SpaceType(String value) {
        this.value = value;
    }

    public abstract float scoreTranslation(float rawScore);

    /**
     * Get space type name in engine
     *
     * @return name
     */
    public String getValue() { return value; }

    public static Set<String> getValues() {
        Set<String> values = new HashSet<>();

        for (SpaceType spaceType : SpaceType.values()) {
            values.add(spaceType.getValue());
        }
        return values;
    }

    public static SpaceType getSpace(String spaceTypeName) {
        for (SpaceType currentSpaceType : SpaceType.values()) {
            if (currentSpaceType.getValue().equalsIgnoreCase(spaceTypeName)) {
                return currentSpaceType;
            }
        }
        throw new IllegalArgumentException("Unable to find space: " + spaceTypeName);
    }
}
