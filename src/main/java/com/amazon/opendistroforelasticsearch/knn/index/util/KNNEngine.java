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

import com.amazon.opendistroforelasticsearch.knn.index.KNNMethod;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethodContext;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public enum KNNEngine {
    NMSLIB("NMSLIB", ".hnsw", Collections.singletonMap(
            "hnsw",
            new KNNMethod(
                    "hnsw",
                    ImmutableSet.of(
                            SpaceTypes.L2,
                            SpaceTypes.L1,
                            SpaceTypes.LINF,
                            SpaceTypes.COSINESIMIL,
                            SpaceTypes.INNER_PRODUCT
                    ),
                    ImmutableMap.of(
                            "m", Integer.class,
                            "ef_construction", Integer.class,
                            "ef_search", Integer.class
                    ),
                    Collections.emptyMap(),
                    false
            )),
            Collections.emptyMap()) {
        @Override
        public String getLatestBuildVersion() {
            return NmsLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return NmsLibVersion.LATEST.indexLibraryVersion();
        }
    },
    FAISS("FAISS", ".faiss", ImmutableMap.of(
            "hnsw",
            new KNNMethod(
                    "HNSW",
                    ImmutableSet.of(
                            SpaceTypes.L2,
                            SpaceTypes.INNER_PRODUCT
                    ),
                    //TODO: verify parameter order is maintained
                    ImmutableMap.of(
                            "m", Integer.class
                    ),
                    Collections.emptyMap(),
                    false
            ), "ivf",
            new KNNMethod(
                    "IVF",
                    ImmutableSet.of(
                            SpaceTypes.L2,
                            SpaceTypes.INNER_PRODUCT
                    ),
                    ImmutableMap.of(
                            "ncentroids", Integer.class
                    ),
                    Collections.emptyMap(),
                    true
            )
    ),
            Collections.singletonMap(
                    SpaceTypes.INNER_PRODUCT, rawScore ->
                            SpaceTypes.INNER_PRODUCT.scoreTranslation(-1*rawScore)
            )) {
        @Override
        public String getLatestBuildVersion() {
            return FAISSLibVersion.LATEST.buildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return FAISSLibVersion.LATEST.indexLibraryVersion();
        }

        @Override
        public String generateMethod(KNNMethodContext knnMethodContext) {
            StringBuilder result = new StringBuilder(this.methods.get(knnMethodContext.getName()).getName());

            Iterator<Object> parameters = knnMethodContext.getParameters().values().iterator();

            while (parameters.hasNext()) {
                result.append(parameters.next().toString());
                if (parameters.hasNext()) {
                    result.append("_");
                }
            }

            if (knnMethodContext.getCourseQuantizer() != null) {
                result.append("(");
                result.append(this.generateMethod(knnMethodContext.getCourseQuantizer()));
                result.append(")");
            }

            if (knnMethodContext.getEncoding() != null) {
                result.append(",");
                result.append(knnMethodContext.getEncoding().getName());

                Iterator<Object> encodingParameters = knnMethodContext.getEncoding().getParameters().values().iterator();
                while (encodingParameters.hasNext()) {
                    result.append(encodingParameters.next().toString());
                    if (encodingParameters.hasNext()) {
                        result.append("_");
                    }
                }
            }

            return result.toString();
        }
    };
    public static final KNNEngine DEFAULT = NMSLIB;

    KNNEngine(String knnEngineName, String extension, Map<String, KNNMethod> methods,
              Map<SpaceTypes, Function<Float, Float>> scoreTranslation) {
        this.knnEngineName = knnEngineName;
        this.extension = extension;
        this.methods = methods;
        this.scoreTranslation = scoreTranslation;
    }

    private String knnEngineName;
    private String extension;
    protected Map<String, KNNMethod> methods;
    private Map<SpaceTypes, Function<Float, Float>> scoreTranslation;

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
        if (this.scoreTranslation.containsKey(spaceTypes)) {
            return this.scoreTranslation.get(spaceTypes).apply(rawScore);
        }

        return spaceTypes.scoreTranslation(rawScore);
    }

    public boolean validateMethod(KNNMethodContext knnMethodContext) {
        if (knnMethodContext.getEngine() != this) {
            return false;
        }

        if (!methods.containsKey(knnMethodContext.getName())) {
            return false;
        }

        return methods.get(knnMethodContext.getName()).validate(knnMethodContext, this);
    }

    public String generateMethod(KNNMethodContext knnMethodContext) {
        return knnMethodContext.getName();
    }
}
