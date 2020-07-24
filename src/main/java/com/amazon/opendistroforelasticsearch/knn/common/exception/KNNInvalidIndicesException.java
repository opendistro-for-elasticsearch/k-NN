/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.common.exception;

import java.util.List;

public class KNNInvalidIndicesException extends RuntimeException {

    private final List<String> invalidIndices;

    public KNNInvalidIndicesException(List<String> invalidIndices, String message) {
        super(message);
        this.invalidIndices = invalidIndices;
    }

    /**
     * Returns the Invalid Index
     *
     * @return invalid index name
     */
    public List<String> getInvalidIndices() {
        return invalidIndices;
    }

    @Override
    public String toString() {
        return "[KNN] " + String.join(",", invalidIndices) + ' ' + super.toString();
    }
}
