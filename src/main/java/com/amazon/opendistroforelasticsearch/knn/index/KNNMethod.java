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

import java.util.Map;
import java.util.Set;

/**
 * KNNMethod is used to define the structure of a method supported by a particular k-NN library. It is used to validate
 * the KNNMethodContext passed in by the user. It is also used to provide superficial string translations.
 */
public class KNNMethod {

    private MethodComponent methodComponent;
    private Set<SpaceType> spaces;
    private Map<String, MethodComponent> encoders;
    private boolean isCoarseQuantizerAvailable;

    /**
     * KNNMethod Constructor
     *
     * @param methodComponent top level method component that is compatible with the underlying library
     * @param spaces set of valid space types that the method supports
     * @param encoders set of encoders that this method supports
     * @param isCoarseQuantizerAvailable whether this method can take a coarseQuantizer
     */
    public KNNMethod(MethodComponent methodComponent, Set<SpaceType> spaces, Map<String, MethodComponent> encoders,
                     boolean isCoarseQuantizerAvailable) {
        this.methodComponent = methodComponent;
        this.spaces = spaces;
        this.encoders = encoders;
        this.isCoarseQuantizerAvailable = isCoarseQuantizerAvailable;
    }

    /**
     * getMainMethodComponent
     *
     * @return mainMethodComponent
     */
    public MethodComponent getMethodComponent() {
        return methodComponent;
    }

    /**
     * Determines whether the provided space is supported for this method
     *
     * @param space to be checked
     * @return true if the space is supported; false otherwise
     */
    public boolean hasSpace(SpaceType space) {
        return spaces.contains(space);
    }

    /**
     * Return the encoder with encoderName as name
     *
     * @param encoderName name of encoder to be looked up
     * @return KNNEncoder corresponding to encoderName
     */
    public MethodComponent getEncoder(String encoderName) {
        if (!encoders.containsKey(encoderName)) {
            throw new IllegalArgumentException("Invalid encoder: " + encoderName);
        }
        return encoders.get(encoderName);
    }

    /**
     * Determine if this method supports a given encoder
     *
     * @param encoderName name of encoder to be looked up
     * @return true if
     */
    public boolean hasEncoder(String encoderName) {
        return encoders.containsKey(encoderName);
    }

    /**
     * isCoarseQuantizerAvailable
     *
     * @return true if coarse quantizer can be used with this method; false otherwise
     */
    public boolean isCoarseQuantizerAvailable() {
        return isCoarseQuantizerAvailable;
    }

    /**
     * Validate that the configured KNNMethodContext is valid for this method
     *
     * @param knnMethodContext to be validated
     */
    public void validate(KNNMethodContext knnMethodContext) {
        if (!hasSpace(knnMethodContext.getSpaceType())) {
            throw new ValidationException();
        }

        // validate the main method and its parameters
        methodComponent.validate(knnMethodContext.getMethodComponent());

        // validate the encoder and its parameters
        KNNMethodContext.MethodComponentContext encoderContext = knnMethodContext.getEncoder();
        if (encoderContext != null) {
            try {
                getEncoder(encoderContext.getName()).validate(encoderContext);
            } catch (IllegalArgumentException iae) {
                throw new ValidationException();
            }
        }
    }

    /**
     * Generate extra parameters that do not go in the method string
     *
     * @param knnMethodContext from where parameters are retrieved
     * @return map of extra parameters
     */
    public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
        return methodComponent.generateExtraParameterMap(knnMethodContext.getMethodComponent().getParameters());
    }
}
