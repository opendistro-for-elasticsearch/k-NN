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
 * Enum contains spaces supported for approximate nearest neighbor search in the k-NN plugin. Each engine is expected
 * to support a subset of these spaces. Validation should be done in the jni layer and an exception should be
 * propagated up to the Java layer. Additionally, naming translations should be done in jni layer as well. For example,
 * nmslib calls the inner_product space "negdotprod". This translation should take place in the nmslib's jni layer.
 */
public enum SpaceTypes {
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
        @Override
        public float scoreTranslation(float rawScore) {
            if (rawScore < 0) {
                return 2 + 1/(1 + rawScore);
            }
            return 1/(1 + rawScore);
            //TODO: Currently this formula does not work, but something like this could provent scores for big numbers
            // from just going to 1 or 2
            // return (float) Math.max(Float.MAX_VALUE/2 + rawScore/2, 0.0);
        }
    },
    HAMMING_BIT("hammingbit") {
        @Override
        public float scoreTranslation(float rawScore) {
            return 1 / (1 + rawScore);
        }
    };

    private final String value;

    SpaceTypes(String value) {
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

        for (SpaceTypes spaceType : SpaceTypes.values()) {
            values.add(spaceType.getValue());
        }
        return values;
    }

    public static SpaceTypes getSpace(String spaceTypeName) {
        for (SpaceTypes currentSpaceType : SpaceTypes.values()) {
            if (currentSpaceType.getValue().equalsIgnoreCase(spaceTypeName)) {
                return currentSpaceType;
            }
        }
        throw new IllegalArgumentException("Unable to find space: " + spaceTypeName);
    }
}
