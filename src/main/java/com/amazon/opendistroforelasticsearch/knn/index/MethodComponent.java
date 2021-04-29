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
import java.util.stream.Collectors;

/**
 * MethodComponent defines the structure of an individual component that can make up an index
 */
public class MethodComponent {

    private String name;
    private Map<String, Parameter<?>> parameters;

    /**
     * Constructor
     *
     * @param builder to build method component
     */
    private MethodComponent(Builder builder) {
        this.name = builder.name;
        this.parameters = builder.parameters;
    }

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
     * Generate extra parameters for this component that are not contained in method string
     *
     * @return map of extra parameters
     */
    public Map<String, Object> generateExtraParameterMap(Map<String, Object> inParameters) {
        if (inParameters == null) {
            return Collections.emptyMap();
        }
        return inParameters.entrySet().stream().filter(v -> !parameters.get(v.getKey()).isInMethodString()).collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Validate that the methodComponentContext is a valid configuration for this methodComponent
     *
     * @param methodComponentContext to be validated
     */
    public void validate(MethodComponentContext methodComponentContext) {
        Map<String, Object> providedParameters = methodComponentContext.getParameters();

        if (providedParameters == null) {
            return;
        }

        for (Map.Entry<String, Object> parameter : providedParameters.entrySet()) {
            if (!parameters.containsKey(parameter.getKey())) {
                throw new ValidationException();
            }

            parameters.get(parameter.getKey()).validate(parameter.getValue());
        }
    }

    /**
     * Builder class for MethodComponent
     */
    public static class Builder {

        private String name;
        private Map<String, Parameter<?>> parameters;

        /**
         * Method to get a Builder instance
         *
         * @param name of method component
         * @return Builder instance
         */
        public static Builder builder(String name) {
            return new MethodComponent.Builder(name);
        }

        private Builder(String name) {
            this.name = name;
            this.parameters = new HashMap<>();
        }

        /**
         * Add parameter entry to parameters map
         *
         * @param parameterName name of the parameter
         * @param parameter parameter to be added
         * @return this builder
         */
        public Builder putParameter(String parameterName, Parameter<?> parameter) {
            this.parameters.put(parameterName, parameter);
            return this;
        }

        /**
         * Build MethodComponent
         *
         * @return Method Component built from builder
         */
        public MethodComponent build() {
            return new MethodComponent(this);
        }
    }
}
