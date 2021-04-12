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
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ValidationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_FLAT;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_PQ;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.FAISS_EXTENSION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.FAISS_NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_IVF;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_CODE_SIZE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NCENTROIDS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NMSLIB_EXTENSION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NMSLIB_NAME;


/**
 * KNNEngine provides the functionality to validate and transform user defined indices into information that can be
 * passed to the respective k-NN library's JNI layer.
 *
 * Included in each engine definition are the supported methods with supported spaces and parameters.
 */
public enum KNNEngine {
    NMSLIB(NMSLIB_NAME, NMSLIB_EXTENSION,
            getNmslibSupportedMethods(),
            Collections.emptyMap(),
            NmsLibVersion.LATEST.getBuildVersion(),
            NmsLibVersion.LATEST.indexLibraryVersion()),
    FAISS(FAISS_NAME, FAISS_EXTENSION,
            getFaissSupportedMethods(),
            Collections.singletonMap(
                    SpaceType.INNER_PRODUCT, rawScore ->
                            SpaceType.INNER_PRODUCT.scoreTranslation(-1*rawScore)
            ),
            FaissLibVersion.LATEST.getBuildVersion(),
            FaissLibVersion.LATEST.indexLibraryVersion()) {
        @Override
        public String generateMethod(KNNMethodContext knnMethodContext) {
            String methodName = knnMethodContext.getMethodComponent().getName();
            KNNMethod knnMethod = methods.get(methodName);

            if (knnMethod == null) {
                throw new IllegalArgumentException("Invalid method for faiss engine: " + methodName);
            }

            StringBuilder methodStringBuilder = new StringBuilder(knnMethod.getMainMethodComponent().getName());

            // Attach all of the parameters for the main method component
            Map<String, Object> parameters = knnMethodContext.getMethodComponent().getParameters();
            String prefix = "";
            for (Map.Entry<String, KNNMethod.Parameter<?>> parameter : knnMethod.getMainMethodComponent()
                    .getParameters().entrySet().stream().filter(m -> m.getValue().isInMethodString())
                    .collect(Collectors.toSet())) {
                methodStringBuilder.append(prefix);
                if (parameters != null && parameters.containsKey(parameter.getKey())) {
                    methodStringBuilder.append(parameters.get(parameter.getKey()));
                } else {
                    methodStringBuilder.append(parameter.getValue().getDefaultValue());
                }
                prefix = "_";
            }

            // Add coarse quantizer if necessary
            if (knnMethodContext.getCoarseQuantizer() != null && !knnMethod.isCoarseQuantizerAvailable()) {
                throw new IllegalArgumentException("Cannot pass coarse quantizer for method: " + methodName);
            } else if (knnMethodContext.getCoarseQuantizer() != null) {
                methodStringBuilder.append("(");
                methodStringBuilder.append(generateMethod(knnMethodContext.getCoarseQuantizer()));
                methodStringBuilder.append(")");
            }

            if (methodStringBuilder.length() > 0) {
                methodStringBuilder.append(",");
            }

            // Add encoding parameters
            KNNMethodContext.MethodComponentContext encoderContext = knnMethodContext.getEncoder();
            if (encoderContext != null && !knnMethod.hasEncoder(encoderContext.getName())) {
                throw new IllegalArgumentException("Invalid encoder: " + encoderContext.getName());
            } else if (encoderContext != null) {
                KNNMethod.MethodComponent encoderComponent = knnMethod.getEncoder(encoderContext.getName());
                methodStringBuilder.append(encoderComponent.getName());
                parameters = encoderContext.getParameters();
                prefix = "";
                for (Map.Entry<String, KNNMethod.Parameter<?>> parameter : encoderComponent.getParameters().entrySet()
                        .stream().filter(m -> m.getValue().isInMethodString()).collect(Collectors.toSet())) {
                    methodStringBuilder.append(prefix);
                    if (parameters != null && parameters.containsKey(parameter.getKey())) {
                        methodStringBuilder.append(parameters.get(parameter.getKey()));
                    } else {
                        methodStringBuilder.append(parameter.getValue().getDefaultValue());
                    }
                    prefix = "_";
                }
            } else {
                methodStringBuilder.append("Flat");
            }

            logger.debug("[KNN] Faiss factory method description: " + methodStringBuilder.toString());
            return methodStringBuilder.toString();
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
     * Get method associated with method name
     *
     * @param methodName name of method to be retrieved
     * @return KNNMethod
     */
    public KNNMethod getMethod(String methodName) {
        if (!methods.containsKey(methodName)) {
            throw new IllegalArgumentException("Invalid method name: " + methodName);
        }
        return methods.get(methodName);
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

        KNNMethod knnMethod = methods.get(methodName);

        if (!knnMethod.hasSpace(knnMethodContext.getSpaceType())) {
            throw new ValidationException();
        }

        knnMethod.getMainMethodComponent().validate(knnMethodContext.getMethodComponent());

        KNNMethodContext coarseQuantizerContext = knnMethodContext.getCoarseQuantizer();
        if (coarseQuantizerContext != null && !knnMethod.isCoarseQuantizerAvailable()) {
            throw new ValidationException();
        } else if (coarseQuantizerContext != null) {
            validateMethod(coarseQuantizerContext);
        }

        KNNMethodContext.MethodComponentContext encoderContext = knnMethodContext.getEncoder();
        if (encoderContext != null) {
            knnMethod.getEncoder(encoderContext.getName()).validate(encoderContext);
        }
    }

    /**
     * Generate method string that will be passed to the library to build the index. By default, the name of the
     * method is returned. This should be overriden if an engine expects a different string description.
     *
     * @param knnMethodContext method definition to produce the string with
     * @return method string to be passed to the engine's library
     */
    public String generateMethod(KNNMethodContext knnMethodContext) {
        String methodName = knnMethodContext.getMethodComponent().getName();
        if (!this.methods.containsKey(methodName)) {
            throw new IllegalArgumentException("Invalid method: " + knnMethodContext.getMethodComponent().getName());
        }

        return this.methods.get(methodName).getMainMethodComponent().getName();
    }

    /**
     * For a given knnMethodContext, generate a map of the extra parameters that the engine's jni layer can use to set
     * parameters that are not passed into the constructor
     *
     * @param knnMethodContext definition to produce the extra parameter map
     * @return extra parameter map
     */
    public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
        String methodName = knnMethodContext.getMethodComponent().getName();
        if (!this.methods.containsKey(methodName)) {
            throw new IllegalArgumentException("Invalid method: " + knnMethodContext.getMethodComponent().getName());
        }

        KNNMethod knnMethod = this.methods.get(methodName);

        Map<String, Object> extraParameterMap = new HashMap<>();
        Map<String, Object> parameters = knnMethodContext.getMethodComponent().getParameters();

        for (Map.Entry<String, KNNMethod.Parameter<?>> parameter : knnMethod.getMainMethodComponent()
                .getParameters().entrySet().stream().filter(m -> !m.getValue().isInMethodString())
                .collect(Collectors.toSet())) {
            if (parameters != null && parameters.containsKey(parameter.getKey())) {
                extraParameterMap.put(parameter.getKey(), parameters.get(parameter.getKey()));
            } else {
                extraParameterMap.put(parameter.getKey(), parameter.getValue().getDefaultValue());
            }
        }

        if (knnMethodContext.getCoarseQuantizer() != null && !knnMethod.isCoarseQuantizerAvailable()) {
            throw new IllegalArgumentException("Cannot pass coarse quantizer for method: " + methodName);
        } else if (knnMethodContext.getCoarseQuantizer() != null) {
            extraParameterMap.put(COARSE_QUANTIZER, generateExtraParameterMap(knnMethodContext.getCoarseQuantizer()));
        }

        return extraParameterMap;
    }

    /**
     * Static method to return the methods that nmslib supports
     *
     * @return Map of method names to methods that nmslib supports
     */
    public static Map<String, KNNMethod> getNmslibSupportedMethods() {
        return Collections.singletonMap(
                METHOD_HNSW, new KNNMethod(
                        "hnsw",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.L1,
                                SpaceType.LINF,
                                SpaceType.COSINESIMIL,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                METHOD_PARAMETER_M, new KNNMethod.Parameter.IntegerParameter(16, false),
                                METHOD_PARAMETER_EF_CONSTRUCTION, new KNNMethod.Parameter.IntegerParameter(512, false)
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
                METHOD_HNSW, new KNNMethod(
                        "HNSW",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                METHOD_PARAMETER_M, new KNNMethod.Parameter.IntegerParameter(16, true)
                        ),
                        getFaissEncoders(),
                        false
                ), METHOD_IVF, new KNNMethod(
                        "IVF",
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ImmutableMap.of(
                                METHOD_PARAMETER_NCENTROIDS, new KNNMethod.Parameter.IntegerParameter(16, true),
                                METHOD_PARAMETER_NPROBES, new KNNMethod.Parameter.IntegerParameter(1, false)
                        ),
                        getFaissEncoders(),
                        true
                ), ENCODER_FLAT, new KNNMethod(
                        "",
                        ImmutableSet.of(SpaceType.L2, SpaceType.INNER_PRODUCT),
                        Collections.emptyMap(),
                        getFaissEncoders(),
                        false
                )
        );
    }

    /**
     * Static method to return the encoders that nmslib supports
     *
     * @return Map of encoders nmslib supports
     */
    public static Map<String, KNNMethod.MethodComponent> getNmslibEncoders() {
        return Collections.emptyMap();
    }

    /**
     * Static method to return the encoders that faiss supports
     *
     * @return Map of encoders faiss supports
     */
    public static Map<String, KNNMethod.MethodComponent> getFaissEncoders() {
        return ImmutableMap.of(
                ENCODER_PQ, new KNNMethod.MethodComponent(
                        "PQ",  ImmutableMap.of(METHOD_PARAMETER_CODE_SIZE, new KNNMethod.Parameter.IntegerParameter(16, true))
                )
        );
    }
}
