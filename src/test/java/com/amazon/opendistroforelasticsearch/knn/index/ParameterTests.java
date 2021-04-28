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

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import org.elasticsearch.common.ValidationException;

public class ParameterTests extends KNNTestCase {
    /**
     * Test default default value getter
     */
    public void testGetDefaultValue() {
        String defaultValue = "test-default";
        Parameter<String> parameter = new Parameter<String>(defaultValue, false, v -> true) {
            @Override
            public void validate(Object value) {}
        };

        assertEquals(defaultValue, parameter.getDefaultValue());
    }


    /**
     * Test default is in method string
     */
    public void testIsInMethodString() {
        boolean inMethodString = false;
        Parameter<String> parameter = new Parameter<String>("", inMethodString, v -> true) {
            @Override
            public void validate(Object value) {}
        };

        assertEquals(inMethodString, parameter.isInMethodString());
    }

    /**
     * Test integer parameter validate
     */
    public void testIntegerParameter_validate() {
        final Parameter.IntegerParameter parameter = new Parameter.IntegerParameter(1, false,
                v -> v > 0);

        // Invalid type
        expectThrows(ValidationException.class, () -> parameter.validate("String"));

        // Invalid value
        expectThrows(ValidationException.class, () -> parameter.validate(-1));

        // valid value
        parameter.validate(12);
    }
}
