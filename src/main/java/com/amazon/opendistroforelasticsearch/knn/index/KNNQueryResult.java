/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * Place holder for the score of the document
 */
public class KNNQueryResult {
    private final int id;
    private final float score;

    public KNNQueryResult(final int id, final float score) {
        this.id = id;
        this.score = score;
    }

    public int getId() {
        return this.id;
    }

    public float getScore() {
        return this.score;
    }
}
