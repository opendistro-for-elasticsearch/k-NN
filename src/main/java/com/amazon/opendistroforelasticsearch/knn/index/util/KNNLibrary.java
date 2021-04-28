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
import com.amazon.opendistroforelasticsearch.knn.index.MethodComponent;
import com.amazon.opendistroforelasticsearch.knn.index.Parameter;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.ValidationException;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_FLAT;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER_PQ;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_IVF;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_CODE_SIZE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_EF_SEARCH;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_M;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NCENTROIDS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_PARAMETER_NPROBES;

/**
 * KNNLibrary is an interface that helps the plugin communicate with k-NN libraries
 */
public interface KNNLibrary {

    /**
     * Gets the library's latest build version
     *
     * @return the string representing the library's latest build version
     */
    String getLatestBuildVersion();

    /**
     * Gets the library's latest version
     *
     * @return the string representing the library's latest version
     */
    String getLatestLibVersion();

    /**
     * Gets the extension that files written with this library should have
     *
     * @return extension
     */
    String getExtension();

    /**
     * Gets the compound extension that files written with this library should have
     *
     * @return compound extension
     */
    String getCompoundExtension();

    /**
     * Gets a particular KNN method that the library supports. This should throw an exception if the method is not
     * supported by the library.
     *
     * @param methodName name of the method to be looked up
     * @return KNNMethod in the library corresponding to the method name
     */
    KNNMethod getMethod(String methodName);

    /**
     * Generate the Lucene score from the rawScore returned by the library. With k-NN, often times the library
     * will return a score where the lower the score, the better the result. This is the opposite of how Lucene scores
     * documents.
     *
     * @param rawScore returned by the library
     * @param spaceType spaceType used to compute the score
     * @return Lucene score for the rawScore
     */
    float score(float rawScore, SpaceType spaceType);

    /**
     * Validate the knnMethodContext for the given library. A ValidationException should be thrown if the method is
     * deemed invalid.
     *
     * @param knnMethodContext to be validated
     */
    void validateMethod(KNNMethodContext knnMethodContext);

    /**
     * Generate the method string that will be passed to the library to build the index. This method string may be used
     * to configure the underlying library index.
     *
     * @param knnMethodContext method definition to produce the string with
     * @return method string to be passed to the engine's library
     */
    String generateMethod(KNNMethodContext knnMethodContext);

    /**
     * Generate the extra parameters that may need to be passed to the library. This function enables passing extra
     * information to the jni to improve index
     *
     * @param knnMethodContext from which extra parameters should be parsed
     * @return Map of the extra parameters
     */
    Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext);

    /**
     * Abstract implementation of KNNLibrary. It contains several default methods and fields that
     * are common across different underlying libraries.
     */
    abstract class NativeLibrary implements KNNLibrary {
        protected Map<String, KNNMethod> methods;
        private Map<SpaceType, Function<Float, Float>> scoreTranslation;
        private String latestLibraryBuildVersion;
        private String latestLibraryVersion;
        private String extension;

        /**
         * Constructor for NativeLibrary
         *
         * @param methods map of methods the native library supports
         * @param scoreTranslation Map of translation of space type to scores returned by the library
         * @param latestLibraryBuildVersion String representation of latest build version of the library
         * @param latestLibraryVersion String representation of latest version of the library
         * @param extension String representing the extension that library files should use
         */
        public NativeLibrary(Map<String, KNNMethod> methods, Map<SpaceType, Function<Float, Float>> scoreTranslation,
                             String latestLibraryBuildVersion, String latestLibraryVersion, String extension)
        {
            this.methods = methods;
            this.scoreTranslation = scoreTranslation;
            this.latestLibraryBuildVersion = latestLibraryBuildVersion;
            this.latestLibraryVersion = latestLibraryVersion;
            this.extension = extension;
        }

        @Override
        public String getLatestBuildVersion() {
            return this.latestLibraryBuildVersion;
        }

        @Override
        public String getLatestLibVersion() {
            return this.latestLibraryVersion;
        }

        @Override
        public String getExtension() {
            return this.extension;
        }

        @Override
        public String getCompoundExtension() {
            return getExtension() + "c";
        }

        @Override
        public KNNMethod getMethod(String methodName) {
            if (!methods.containsKey(methodName)) {
                throw new IllegalArgumentException("Invalid method name: " + methodName);
            }
            return methods.get(methodName);
        }

        @Override
        public float score(float rawScore, SpaceType spaceType) {
            if (this.scoreTranslation.containsKey(spaceType)) {
                return this.scoreTranslation.get(spaceType).apply(rawScore);
            }

            return spaceType.scoreTranslation(rawScore);
        }

        @Override
        public void validateMethod(KNNMethodContext knnMethodContext) {
            String methodName = knnMethodContext.getMethodComponent().getName();
            if (!methods.containsKey(methodName)) {
                throw new ValidationException();
            }

            KNNMethod knnMethod = methods.get(methodName);
            knnMethod.validate(knnMethodContext);

            KNNMethodContext coarseQuantizerContext = knnMethodContext.getCoarseQuantizer();
            if (coarseQuantizerContext != null && !knnMethod.isCoarseQuantizerAvailable()) {
                throw new ValidationException();
            } else if (coarseQuantizerContext != null) {
                validateMethod(coarseQuantizerContext);
            }
        }

        @Override
        public String generateMethod(KNNMethodContext knnMethodContext) {
            String methodName = knnMethodContext.getMethodComponent().getName();

            if (!this.methods.containsKey(methodName)) {
                throw new IllegalArgumentException("Invalid method: " + knnMethodContext.getMethodComponent().getName());
            }

            return this.methods.get(methodName).getMethodComponent().getName();
        }

        @Override
        public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
            String methodName = knnMethodContext.getMethodComponent().getName();
            if (!this.methods.containsKey(methodName)) {
                throw new IllegalArgumentException("Invalid method: " + knnMethodContext.getMethodComponent().getName());
            }

            KNNMethod knnMethod = this.methods.get(methodName);
            Map<String, Object> extraParameterMap = knnMethod.generateExtraParameterMap(knnMethodContext);

            if (knnMethodContext.getCoarseQuantizer() != null && !knnMethod.isCoarseQuantizerAvailable()) {
                throw new IllegalArgumentException("Cannot pass coarse quantizer for method: " + methodName);
            } else if (knnMethodContext.getCoarseQuantizer() != null) {
                extraParameterMap.put(COARSE_QUANTIZER, generateExtraParameterMap(knnMethodContext.getCoarseQuantizer()));
            }

            return extraParameterMap;
        }
    }

    /**
     * Implements NativeLibrary for the nmslib native library
     */
    class Nmslib extends NativeLibrary {

        public final static Map<String, MethodComponent> ENCODERS = Collections.emptyMap();

        public final static Map<String, KNNMethod> METHODS = ImmutableMap.of(
                METHOD_HNSW, new KNNMethod(
                        new MethodComponent.Builder("hnsw")
                                .putParameter(METHOD_PARAMETER_M, new Parameter.IntegerParameter(16, false,
                                        v -> v > 0))
                                .putParameter(METHOD_PARAMETER_EF_CONSTRUCTION, new Parameter.IntegerParameter(512,
                                                false, v -> v > 0))
                                .build(),
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.L1,
                                SpaceType.LINF,
                                SpaceType.COSINESIMIL,
                                SpaceType.INNER_PRODUCT
                        ),
                        ENCODERS,
                        false)
        );

        public final static Map<SpaceType, Function<Float, Float>> SCORE_TRANSLATIONS = Collections.emptyMap();

        public final static String LATEST_LIBRARY_BUILD_VERSION = NmsLibVersion.LATEST.getBuildVersion();
        public final static String LATEST_LIBRARY_VERSION = NmsLibVersion.LATEST.indexLibraryVersion();
        public final static String EXTENSION = ".hnsw";

        public final static Nmslib INSTANCE = new Nmslib(METHODS, SCORE_TRANSLATIONS, LATEST_LIBRARY_BUILD_VERSION,
                LATEST_LIBRARY_VERSION, EXTENSION);

        /**
         * Constructor for Nmslib
         *
         * @param methods Set of methods the native library supports
         * @param scoreTranslation Map of translation of space type to scores returned by the library
         * @param latestLibraryBuildVersion String representation of latest build version of the library
         * @param latestLibraryVersion String representation of latest version of the library
         * @param extension String representing the extension that library files should use
         */
        private Nmslib(Map<String, KNNMethod> methods, Map<SpaceType, Function<Float, Float>> scoreTranslation,
                       String latestLibraryBuildVersion, String latestLibraryVersion, String extension) {
            super(methods, scoreTranslation, latestLibraryBuildVersion, latestLibraryVersion, extension);
        }
    }

    /**
     * Implements NativeLibrary for the faiss native library
     */
    class Faiss extends NativeLibrary {

        public final static Map<String, MethodComponent> ENCODERS = ImmutableMap.of(
                ENCODER_PQ, new MethodComponent.Builder("PQ")
                        .putParameter(METHOD_PARAMETER_CODE_SIZE, new Parameter.IntegerParameter(16, true,
                                v -> v > 0))
                        .build(),
                ENCODER_FLAT, new MethodComponent.Builder("Flat")
                        .build()
        );

        public final static Map<String, KNNMethod> METHODS = ImmutableMap.of(
                METHOD_HNSW, new KNNMethod(
                        new MethodComponent.Builder("HNSW")
                                .putParameter(METHOD_PARAMETER_M, new Parameter.IntegerParameter(16, true,
                                                v -> v > 0))
                                .putParameter(METHOD_PARAMETER_EF_CONSTRUCTION, new Parameter.IntegerParameter(512,
                                        false, v -> v > 0))
                                .putParameter(METHOD_PARAMETER_EF_SEARCH, new Parameter.IntegerParameter(512,
                                        false, v -> v > 0))
                                .build(),
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ENCODERS,
                        false
                ), METHOD_IVF, new KNNMethod(
                        new MethodComponent.Builder("IVF")
                                .putParameter(METHOD_PARAMETER_NCENTROIDS, new Parameter.IntegerParameter(16, true,
                                        v -> v > 0))
                                .putParameter(METHOD_PARAMETER_NPROBES, new Parameter.IntegerParameter(1, false,
                                        v -> v > 0))
                                .build(),
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT
                        ),
                        ENCODERS,
                        true
                ), ENCODER_FLAT, new KNNMethod(
                        new MethodComponent.Builder("")
                                .build(),
                        ImmutableSet.of(
                                SpaceType.L2,
                                SpaceType.INNER_PRODUCT),
                        ENCODERS,
                        false
                )
        );

        public final static Map<SpaceType, Function<Float, Float>> SCORE_TRANSLATIONS = ImmutableMap.of(
                SpaceType.INNER_PRODUCT, rawScore ->
                        SpaceType.INNER_PRODUCT.scoreTranslation(-1*rawScore)
        );

        public final static String LATEST_LIBRARY_BUILD_VERSION = FaissLibVersion.LATEST.getBuildVersion();
        public final static String LATEST_LIBRARY_VERSION = FaissLibVersion.LATEST.indexLibraryVersion();
        public final static String EXTENSION = ".faiss";

        public final static Faiss INSTANCE = new Faiss(METHODS, SCORE_TRANSLATIONS, LATEST_LIBRARY_BUILD_VERSION,
                LATEST_LIBRARY_VERSION, EXTENSION);

        /**
         * Constructor for Faiss
         *
         * @param methods Set of methods the native library supports
         * @param scoreTranslation Map of translation of space type to scores returned by the library
         * @param latestLibraryBuildVersion String representation of latest build version of the library
         * @param latestLibraryVersion String representation of latest version of the library
         * @param extension String representing the extension that library files should use
         */
        private Faiss(Map<String, KNNMethod> methods, Map<SpaceType, Function<Float, Float>> scoreTranslation,
                     String latestLibraryBuildVersion, String latestLibraryVersion, String extension) {
            super(methods, scoreTranslation, latestLibraryBuildVersion, latestLibraryVersion, extension);
        }

        @Override
        public String generateMethod(KNNMethodContext knnMethodContext) {
            String methodName = knnMethodContext.getMethodComponent().getName();
            KNNMethod knnMethod = methods.get(methodName);

            if (knnMethod == null) {
                throw new IllegalArgumentException("Invalid method for faiss engine: " + methodName);
            }

            StringBuilder methodStringBuilder = new StringBuilder(knnMethod.getMethodComponent().getName());

            // Attach all of the parameters for the main method component
            Map<String, Object> parameters = knnMethodContext.getMethodComponent().getParameters();
            String prefix = "";
            for (Map.Entry<String, Parameter<?>> parameter : knnMethod.getMethodComponent()
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
                MethodComponent encoderComponent = knnMethod.getEncoder(encoderContext.getName());
                methodStringBuilder.append(encoderComponent.getName());
                parameters = encoderContext.getParameters();
                prefix = "";
                for (Map.Entry<String, Parameter<?>> parameter : encoderComponent.getParameters().entrySet()
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

            return methodStringBuilder.toString();
        }
    }
}
