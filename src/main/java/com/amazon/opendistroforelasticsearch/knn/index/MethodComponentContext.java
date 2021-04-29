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

import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;

/**
 * MethodComponentContext represents a single user provided building block of a knn library index.
 *
 * Each component is composed of a name and a map of parameters.
 */
public class MethodComponentContext implements ToXContentFragment {

    private String name;
    private Map<String, Object> parameters;

    /**
     * Constructor
     *
     * @param name component name
     * @param parameters component parameters
     */
    public MethodComponentContext(String name, Map<String, Object> parameters) {
        this.name = name;
        this.parameters = parameters;
    }

    /**
     * Parses the object into MethodComponentContext
     *
     * @param in Object to be parsed
     * @return MethodComponentContext
     */
    public static MethodComponentContext parse(Object in) {
        if (!(in instanceof Map)) {
            throw new MapperParsingException("Unable to parse MethodComponent");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> methodMap = (Map<String, Object>) in;
        String name = "";
        Map<String, Object> parameters = null;

        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (NAME.equals(key)) {
                if (!(value instanceof String)) {
                    throw new MapperParsingException("Component name should be a string");
                }

                name = (String) value;
            } else if (PARAMETERS.equals(key)) {
                if (!(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse parameters for main method component");
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> parameters1 = (Map<String, Object>) value;
                parameters = parameters1;
            } else {
                throw new MapperParsingException("Invalid parameter for MethodComponentContext: " + key);
            }
        }

        if (name.isEmpty()) {
            throw new MapperParsingException(NAME + " needs to be set");
        }

        return new MethodComponentContext(name, parameters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, name);
        builder.field(PARAMETERS, parameters);
        return builder;
    }

    /**
     * Gets the name of the component
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the parameters of the component
     *
     * @return parameters
     */
    public Map<String, Object> getParameters() {
        return parameters;
    }
}
