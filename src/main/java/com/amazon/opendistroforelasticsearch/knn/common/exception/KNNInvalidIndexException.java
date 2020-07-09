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

public class KNNInvalidIndexException extends RuntimeException {

    private final String invalidIndex;

    public KNNInvalidIndexException(String invalidIndex, String message) {
        super(message);
        this.invalidIndex = invalidIndex;
    }

    /**
     * Returns the Invalid Index
     *
     * @return invalid index name
     */
    public String getInvalidIndex() {
        return this.invalidIndex;
    }

    @Override
    public String toString() {
        return "[KNN] " + invalidIndex + ' ' + super.toString();
    }
}