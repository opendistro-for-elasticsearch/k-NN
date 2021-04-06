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

/**
 * Class defines structure of a parameter that an engine's method takes.
 *
 * @param <T> parameter type
 */
public abstract class MethodParameter<T> {
    /**
     * Constructor
     *
     * @param defaultValue of the parameter
     * @param inMethodString whether the parameter is included in method string
     */
    public MethodParameter(T defaultValue, boolean inMethodString) {
        this.defaultValue = defaultValue;
        this.inMethodString = inMethodString;
    }

    private T defaultValue;
    private boolean inMethodString;

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
     * Check if the value passed in matches the parameter type
     *
     * @param value to be checked
     * @return true if the type is correct; false otherwise
     */
    public abstract boolean checkType(Object value);

    /**
     * Integer method parameter
     */
    public static class IntegerMethodParameter extends MethodParameter<Integer> {
        public IntegerMethodParameter(Integer defaultValue, boolean partOfMethodString) {
            super(defaultValue, partOfMethodString);
        }

        @Override
        public boolean checkType(Object value) {
            return value == null || value instanceof Integer;
        }
    }
}
