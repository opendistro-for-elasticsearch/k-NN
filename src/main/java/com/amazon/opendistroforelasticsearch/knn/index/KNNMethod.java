/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.index;

import org.elasticsearch.common.ValidationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COURSE_QUANTIZER;

/**
 * KNNMethod is used to define the structure of a method supported by a particular k-NN library. It is used to validate
 * the KNNMethodContext passed in by the user. It is also used to provide superficial string translations.
 */
public class KNNMethod {

    /**
     * KNNMethod Constructor
     *
     * @param name name of the method that is compatible with underlying library
     * @param validSpaces set of valid space types that the method supports
     * @param parameters Map of parameters that the method requires
     * @param validEncoders set of encoders that this method supports
     * @param isCourseQuantizerAvailable whether this method can take a courseQuantizer
     */
    public KNNMethod(String name, Set<SpaceType> validSpaces, Map<String, MethodParameter<?>> parameters,
                     Map<String, KNNEncoder> validEncoders, boolean isCourseQuantizerAvailable) {
        this.name = name;
        this.validSpaces = validSpaces;
        this.parameters = parameters;
        this.validEncoders = validEncoders;
        this.isCourseQuantizerAvailable = isCourseQuantizerAvailable;
    }

    private String name;
    private Set<SpaceType> validSpaces;
    private Map<String, MethodParameter<?>> parameters;
    private Map<String, KNNEncoder> validEncoders;
    private boolean isCourseQuantizerAvailable;

    /**
     * Validate that the knnMethodContext is compatible with this defined method structure. The method will throw a
     * ValidationException if the knnMethodContext is deemed to be incompatible.
     *
     * @param knnMethodContext to be validated
     */
    public void validate(KNNMethodContext knnMethodContext) {
        // Validate that the parameters passed in for the main index are supported by this method and have the
        // correct type
        Map<String, Object> methodParameters = knnMethodContext.getMethodComponent().getParameters();
        if (methodParameters != null) {
            for (Map.Entry<String, Object> parameter : methodParameters.entrySet()) {
                if (!parameters.containsKey(parameter.getKey())) {
                    throw new ValidationException();
                }

                if (!parameters.get(parameter.getKey()).checkType(parameter.getValue())) {
                    throw new ValidationException();
                }
            }
        }


        // If a course quantizer is provided by knnMethodContext, recursively validate the course quantizer
        KNNMethodContext courseQuantizer = knnMethodContext.getCourseQuantizer();
        if (courseQuantizer != null) {
            if (!isCourseQuantizerAvailable) {
                throw new ValidationException();
            }
            knnMethodContext.getEngine().validateMethod(courseQuantizer);
        }

        // Check if the provided encoder is valid
        KNNMethodContext.MethodComponentContext encoderContext = knnMethodContext.getEncoder();

        if (encoderContext != null) {
            if (!validEncoders.containsKey(encoderContext.getName())) {
                throw new ValidationException();
            }

            validEncoders.get(encoderContext.getName()).validate(encoderContext);
        }
    }

    /**
     * Return the encoder with encoderName as name
     *
     * @param encoderName name of encoder to be looked up
     * @return KNNEncoder corresponding to encoderName
     */
    public KNNEncoder getEncoder(String encoderName) {
        if (!validEncoders.containsKey(encoderName)) {
            throw new IllegalArgumentException("Invalid encoder: " + encoderName);
        }
        return validEncoders.get(encoderName);
    }

    /**
     * getName
     *
     * @return name of method
     */
    public String getName() {
        return name;
    }

    /**
     * Get valid parameters for this method
     *
     * @return valid parameters for this method
     */
    public Map<String, MethodParameter<?>> getParameters() {
        return parameters;
    }

    /**
     * Determines whether the parameter should be included as a part of the method string
     *
     * @param parameter to be checked
     * @return true if the parameter should be included in the method string passed to the jni layer; false otherwise
     */
    public boolean isParameterInMethodString(String parameter) {
        return parameters.containsKey(parameter) && parameters.get(parameter).isInMethodString();
    }

    /**
     * Generates a map of the extra parameters that may not be included as a part of the method string. This function
     * is called recursively in order to support sub-indices used for the courseQuantizer
     *
     * @param knnMethodContext to generate map for
     * @return Map of extra parameters that can be passed to jni
     */
    public Map<String, Object> generateExtraParametersMap(KNNMethodContext knnMethodContext) {
        if (knnMethodContext == null) {
            return Collections.emptyMap();
        }

        // Filter out invalid parameters and parameters that are included as a part of the method string
        Map<String, Object> extraParameters = new HashMap<>(knnMethodContext.getMethodComponent().getParameters()
                .entrySet().stream()
                .filter(m -> parameters.containsKey(m.getKey()) && !parameters.get(m.getKey()).isInMethodString())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        KNNMethodContext courseQuantizer = knnMethodContext.getCourseQuantizer();
        if (courseQuantizer != null) {
            extraParameters.put(COURSE_QUANTIZER, generateExtraParametersMap(courseQuantizer));
        }

        return extraParameters;
    }
}
