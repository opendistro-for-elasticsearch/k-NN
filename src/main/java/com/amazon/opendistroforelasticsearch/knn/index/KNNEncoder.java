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

/**
 * This needs to be refactored a little bit
 *
 * What is it's purpose?
 *
 * This is really not different than any component. I dont think we need a separate class for this
 */
public class KNNEncoder {

    public KNNEncoder(String name, Map<String, MethodParameter<?>> validParameters) {
        this.name = name;
        this.validParameters = validParameters;
    }

    // An encoder will need a Name, a set of allowed parameters
    protected String name;
    private Map<String, MethodParameter<?>> validParameters;


    public boolean validate(KNNMethodContext.MethodComponentContext encoderContext) {
        for (Map.Entry<String, Object> parameter : encoderContext.getParameters().entrySet()) {
            if (!validParameters.containsKey(parameter.getKey())) {
                return false;
            }

            if (validParameters.get(parameter.getKey()).checkType(parameter.getValue())) {
                return false;
            }
        }
        return true;
    }

    public String buildString(KNNMethodContext.MethodComponentContext encoderContext) {
        return name;
    }
}
