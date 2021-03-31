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

import java.util.Map;

public class KNNEncoder {

    public KNNEncoder(String name, Map<String, Class<?>> validParameters) {
        this.name = name;
        this.validParameters = validParameters;
    }

    // An encoder will need a Name, a set of allowed parameters
    protected String name;
    private Map<String, Class<?>> validParameters;


    public boolean validate(KNNMethodContext.ComponentContext encoderContext) {
        // Validate parameters
        for (Map.Entry<String, Object> parameter : encoderContext.getParameters().entrySet()) {
            if (!validParameters.containsKey(parameter.getKey())) {
                return false;
            }

            if (validParameters.get(parameter.getKey()) != parameter.getValue().getClass()) {
                return false;
            }
        }
        return true;
    }

    public String buildString(KNNMethodContext.ComponentContext encoderContext) {
        return name;
    }
}
