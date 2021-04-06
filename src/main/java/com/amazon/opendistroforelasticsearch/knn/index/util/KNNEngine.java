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

import com.amazon.opendistroforelasticsearch.knn.index.MethodParameter;
import com.amazon.opendistroforelasticsearch.knn.index.KNNEncoder;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethod;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethodContext;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ValidationException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * KNNEngine provides the functionality to validate and transform user defined indices into information that can be
 * passed to the respective k-NN library's JNI layer.
 *
 * Included in each engine definition are the supported methods with supported spaces and parameters.
 */
public enum KNNEngine {
    NMSLIB("NMSLIB", ".hnsw",
            getNmslibSupportedMethods(),
            Collections.emptyMap(),
            NmsLibVersion.LATEST.getBuildVersion(),
            NmsLibVersion.LATEST.indexLibraryVersion()),
    FAISS("FAISS", ".faiss",
            getFaissSupportedMethods(),
            Collections.singletonMap(
                    SpaceType.INNER_PRODUCT, rawScore ->
                            SpaceType.INNER_PRODUCT.scoreTranslation(-1*rawScore)
            ),
            FAISSLibVersion.LATEST.getBuildVersion(),
            FAISSLibVersion.LATEST.indexLibraryVersion()) {
                @Override
                public String generateMethod(KNNMethodContext knnMethodContext) {
                    KNNMethod method = this.methods.get(knnMethodContext.getMethodComponent().getName());
                    String methodName = this.methods.get(knnMethodContext.getMethodComponent().getName()).getName();
                    StringBuilder result = new StringBuilder(methodName);

                    Map<String, Object> parameters = knnMethodContext.getMethodComponent().getParameters();
                    String prefix = "";
                    for (Map.Entry<String, MethodParameter<?>> parameter : method.getParameters().entrySet().stream()
                            .filter(m -> m.getValue().isInMethodString())
                            .collect(Collectors.toSet())) {
                        result.append(prefix);
                        if (parameters != null && parameters.containsKey(parameter.getKey())) {
                            result.append(parameters.get(parameter.getKey()));
                        } else {
                            result.append(parameter.getValue().getDefaultValue());
                        }
                        prefix = "_";
                    }

                    if (knnMethodContext.getCourseQuantizer() != null) {
                        result.append("(");
                        result.append(this.generateMethod(knnMethodContext.getCourseQuantizer()));
                        result.append(")");
                    }

                    if (result.length() > 0) {
                        result.append(",");
                    }

                    if (knnMethodContext.getEncoder() != null) {
                        result.append(method.getEncoder(knnMethodContext.getEncoder().getName())
                                .buildString(knnMethodContext.getEncoder()));
                    } else {
                        result.append("Flat");
                    }

                    logger.debug("[KNN] Faiss factory method description: " + result.toString());
                    return result.toString();
                }
            };

    public static final KNNEngine DEFAULT = NMSLIB;

    /**
     * Constructor for KNNEngine
     *
     * @param name name of the engine
     * @param extension file extension for engine
     * @param methods Map of methods that this engine supports
     * @param scoreTranslation Map of score translation overrides that should be applied on the scores returned by the
     *                         engine's library for a given spaceType.
     */
    KNNEngine(String name, String extension, Map<String, KNNMethod> methods,
              Map<SpaceType, Function<Float, Float>> scoreTranslation, String latestBuildVersion,
              String latestLibVersion) {
        this.name = name;
        this.extension = extension;
        this.methods = methods;
        this.scoreTranslation = scoreTranslation;
        this.latestBuildVersion = latestBuildVersion;
        this.latestLibVersion = latestLibVersion;
    }

    private String name;
    private String extension;
    private Map<SpaceType, Function<Float, Float>> scoreTranslation;
    private String latestBuildVersion;
    private String latestLibVersion;

    protected Map<String, KNNMethod> methods;

    private static Logger logger = LogManager.getLogger(KNNEngine.class);

    public String getLatestBuildVersion() {
        return latestBuildVersion;
    }

    public String getLatestLibVersion() {
        return latestLibVersion;
    }

    /**
     * Get the name of the engine
     *
     * @return name of the engine
     */
    public String getName() {
        return name;
    }

    /**
     * Get the file extension for the engine
     *
     * @return file extension
     */
    public String getExtension() {
        return extension;
    }

    /**
     * Get the compound file extension for the engine
     *
     * @return compound file extension
     */
    public String getCompoundExtension() {
        return extension + "c";
    }

    /**
     * Get the engine
     * @param name of engine to be fetched
     * @return KNNEngine corresponding to name
     */
    public static KNNEngine getEngine(String name) {
        if(FAISS.getName().equalsIgnoreCase(name)) {
            return FAISS;
        } else if (NMSLIB.getName().equalsIgnoreCase(name)){
            return NMSLIB;
        }
        throw new IllegalArgumentException("[KNN] Invalid engine type: " + name);
    }

    /**
     * Generate the Elasticsearch score from the rawScore returned by the engine.
     *
     * @param rawScore returned by the engine
     * @param spaceType spaceType used to compute the score
     * @return Elasticsearch score for the rawScore
     */
    public float score(float rawScore, SpaceType spaceType) {
        if (this.scoreTranslation.containsKey(spaceType)) {
            return this.scoreTranslation.get(spaceType).apply(rawScore);
        }

        return spaceType.scoreTranslation(rawScore);
    }

    /**
     * Validate the knnMethodContext for the given engine
     *
     * @param knnMethodContext to be validated
     */
    public void validateMethod(KNNMethodContext knnMethodContext) {
        if (knnMethodContext.getEngine() != this) {
            throw new ValidationException();
        }

        String methodName = knnMethodContext.getMethodComponent().getName();
        if (!methods.containsKey(methodName)) {
            throw new ValidationException();
        }

        methods.get(methodName).validate(knnMethodContext);
    }

    /**
     * Generate method string that will be passed to the library to build the index
     *
     * @param knnMethodContext method definition to produce the string with
     * @return method string to be passed to the engine's library
     */
    public String generateMethod(KNNMethodContext knnMethodContext) {
        return knnMethodContext.getMethodComponent().getName();
    }

    /**
     * For a given knnMethodContext, generate a map of the extra parameters that the engine's jni layer can use to set
     * parameters that are not passed into the constructor
     *
     * @param knnMethodContext definition to produce the extra parameter map
     * @return extra parameter map
     */
    public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
        KNNMethod method = this.methods.get(knnMethodContext.getMethodComponent().getName());
        return method.generateExtraParametersMap(knnMethodContext);
    }

    /**
     * Static method to return the methods that nmslib supports
     *
     * @return Map of method names to methods that nmslib supports
     */
    public static Map<String, KNNMethod> getNmslibSupportedMethods() {
        return Collections.singletonMap(
                "hnsw", new KNNMethod("hnsw",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.L1,
                                SpaceType.LINF,
                                SpaceType.COSINESIMIL,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                "m", new MethodParameter.IntegerMethodParameter(16, false),
                                "ef_construction", new MethodParameter.IntegerMethodParameter(512, false),
                                "ef_search", new MethodParameter.IntegerMethodParameter(512, false)
                        ),
                        getNmslibEncoders(),
                        false
                )
        );
    }

    /**
     * Static method to return the methods that faiss supports
     *
     * @return Map of method names to methods that faiss supports
     */
    public static Map<String, KNNMethod> getFaissSupportedMethods() {
        return ImmutableMap.of(
                "hnsw", new KNNMethod(
                        "HNSW",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                "m", new MethodParameter.IntegerMethodParameter(16, true)
                        ),
                        getFaissEncoders(),
                        false
                ), "ivf",
                new KNNMethod(
                        "IVF",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                "ncentroids", new MethodParameter.IntegerMethodParameter(16, true),
                                "nprobes", new MethodParameter.IntegerMethodParameter(1, false)
                        ),
                        getFaissEncoders(),
                        true
                ), "flat", new KNNMethod("", ImmutableSet.of(SpaceType.L2, SpaceType.INNER_PRODUCT),
                        Collections.emptyMap(), Collections.emptyMap(), false));
    }

    /**
     * Static method to return the encoders that nmslib supports
     *
     * @return Map of encoders nmslib supports
     */
    public static Map<String, KNNEncoder> getNmslibEncoders() {
        return Collections.emptyMap();
    }

    /**
     * Static method to return the encoders that faiss supports
     *
     * @return Map of encoders faiss supports
     */
    public static Map<String, KNNEncoder> getFaissEncoders() {
        return ImmutableMap.of(
                "pq", new KNNEncoder(
                        "PQ",  ImmutableMap.of("code_size", new MethodParameter.IntegerMethodParameter(16, true))
                ) {
                    @Override
                    public String buildString(KNNMethodContext.MethodComponentContext encoderContext) {
                        StringBuilder result = new StringBuilder(this.name);

                        Iterator<Object> parameters = encoderContext.getParameters().values().iterator();

                        while (parameters.hasNext()) {
                            result.append(parameters.next().toString());
                            if (parameters.hasNext()) {
                                result.append("_");
                            }
                        }
                        return result.toString();
                    }
                }
        );
    }
}
