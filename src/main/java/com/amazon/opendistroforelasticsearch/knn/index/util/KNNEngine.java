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

package com.amazon.opendistroforelasticsearch.knn.index.util;

import com.amazon.opendistroforelasticsearch.knn.index.SpaceTypes;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public enum KNNEngine {
    NMSLIB("NMSLIB", ".hnsw", Collections.emptyMap()) {
        @Override
        public String getLatestBuildVersion() {
            return NmsLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return NmsLibVersion.LATEST.indexLibraryVersion();
        }
    },
    FAISS("FAISS", ".faiss", Collections.singletonMap(
            SpaceTypes.INNER_PRODUCT, rawScore -> SpaceTypes.INNER_PRODUCT.scoreTranslation(-1*rawScore))) {
        @Override
        public String getLatestBuildVersion() {
            return FAISSLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return FAISSLibVersion.LATEST.indexLibraryVersion();
        }
    };
    public static final KNNEngine DEFAULT = NMSLIB;

    KNNEngine(String knnEngineName, String extension, Map<SpaceTypes, Function<Float, Float>> scoreOverride) {
        this.knnEngineName = knnEngineName;
        this.extension = extension;
        this.scoreOverride = scoreOverride;
    }

    private String knnEngineName;
    private String extension;
    private Map<SpaceTypes, Function<Float, Float>> scoreOverride;

    public abstract String getLatestBuildVersion();
    public abstract String getLatestLibVersion();

    public String getKnnEngineName() {
        return knnEngineName;
    }

    public String getExtension() {
        return extension;
    }

    public String getCompoundExtension() {
        return extension + "c";
    }

    public static Set<String> getEngines() {
        Set<String> values = new HashSet<>();
        for (KNNEngine engineName : KNNEngine.values()) {
            values.add(engineName.getKnnEngineName());
        }
        return values;
    }

    public static KNNEngine getEngine(String name) {
        if(FAISS.knnEngineName.equalsIgnoreCase(name)) {
            return FAISS;
        } else if (NMSLIB.knnEngineName.equalsIgnoreCase(name)){
            return NMSLIB;
        }
        throw new IllegalArgumentException("[KNN] Invalid engine type: " + name);
    }

    public float score(float rawScore, SpaceTypes spaceTypes) {
        if (this.scoreOverride.containsKey(spaceTypes)) {
            return this.scoreOverride.get(spaceTypes).apply(rawScore);
        }

        return spaceTypes.scoreTranslation(rawScore);
    }
}
