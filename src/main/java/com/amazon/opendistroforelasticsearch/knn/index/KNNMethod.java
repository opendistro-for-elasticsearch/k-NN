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
import java.util.function.Function;

/**
 * KNNMethod is used to define the structure of a method supported by a particular k-NN library. It is used to validate
 * the KNNMethodContext passed in by the user. It is also used to provide superficial string translations.
 */
public class KNNMethod {

    /**
     * KNNMethod Constructor
     *
     * @param name name of the method that is compatible with underlying library
     * @param spaces set of valid space types that the method supports
     * @param parameters Map of parameters that the method requires
     * @param encoders set of encoders that this method supports
     * @param isCoarseQuantizerAvailable whether this method can take a coarseQuantizer
     */
    public KNNMethod(String name, Set<SpaceType> spaces, Map<String, Parameter<?>> parameters,
                     Map<String, MethodComponent> encoders, boolean isCoarseQuantizerAvailable) {
        this.methodComponent = new MethodComponent(name, parameters);
        this.spaces = spaces;
        this.encoders = encoders;
        this.isCoarseQuantizerAvailable = isCoarseQuantizerAvailable;
    }

    private MethodComponent methodComponent;
    private Set<SpaceType> spaces;
    private Map<String, MethodComponent> encoders;
    private boolean isCoarseQuantizerAvailable;

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
     * MethodComponent defines the structure of an individual component that can make up an index
     */
    public static class MethodComponent {

        /**
         * Constructor
         * @param name name of component
         * @param parameters parameters that the component can take
         */
        public MethodComponent(String name, Map<String, Parameter<?>> parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        protected String name;
        private Map<String, Parameter<?>> parameters;

        /**
         * Get the name of the component
         *
         * @return name
         */
        public String getName() {
            return name;
        }

        /**
         * Get the parameters for the component
         *
         * @return parameters
         */
        public Map<String, Parameter<?>> getParameters() {
            return parameters;
        }

        /**
         * Validate that the methodComponentContext is a valid configuration for this methodComponent
         *
         * @param methodComponentContext to be validated
         */
        public void validate(KNNMethodContext.MethodComponentContext methodComponentContext) {
            Map<String, Object> providedParameters = methodComponentContext.getParameters();
            if (providedParameters != null) {
                for (Map.Entry<String, Object> parameter : providedParameters.entrySet()) {
                    if (!parameters.containsKey(parameter.getKey())) {
                        throw new ValidationException();
                    }

                    if (!parameters.get(parameter.getKey()).validate(parameter.getValue())) {
                        throw new ValidationException();
                    }
                }
            }
        }
    }

    /**
     * Parameter that can be set for a method component
     *
     * @param <T> Type parameter takes
     */
    public static abstract class Parameter<T> {
        /**
         * Constructor
         *
         * @param defaultValue of the parameter
         * @param inMethodString whether the parameter is included in method string
         */
        public Parameter(T defaultValue, boolean inMethodString, Function<T, Boolean> validator) {
            this.defaultValue = defaultValue;
            this.inMethodString = inMethodString;
            this.validator = validator;
        }

        private T defaultValue;
        private boolean inMethodString;
        protected Function<T, Boolean> validator;

        /**
         * Get default value for parameter
         *
         * @return default value of the parameter
         */
        public T getDefaultValue() {
            return defaultValue;
        }

        /**
         * Is the parameter included in the method string
         *
         * @return true if the parameter should be included in the method string; false otherwise
         */
        public boolean isInMethodString() {
            return inMethodString;
        }

        /**
         * Check if the value passed in is valid
         *
         * @param value to be checked
         * @return true if the value is valid; false otherwise
         */
        public abstract boolean validate(Object value);

        /**
         * Integer method parameter
         */
        public static class IntegerParameter extends Parameter<Integer> {
            public IntegerParameter(Integer defaultValue, boolean inMethodString, Function<Integer, Boolean> validator)
            {
                super(defaultValue, inMethodString, validator);
            }

            @Override
            public boolean validate(Object value) {
                if (!(value instanceof Integer)) {
                    return false;
                }
                return validator.apply((Integer) value);
            }
        }
    }
}
