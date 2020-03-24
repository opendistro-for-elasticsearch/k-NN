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

package com.amazon.opendistroforelasticsearch.knn.index.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.MergeState;

import java.io.IOException;

/**
 * Place holder class for the docIds and the respective
 * binary values.
 */
public class BinaryDocValuesSub extends DocIDMerger.Sub {

    private final BinaryDocValues values;

    public BinaryDocValues getValues() {
        return values;
    }

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values) {
        super(docMap);
        if (values == null || (values.docID() != -1)) {
            throw new IllegalStateException("Doc values is either null or docID is not -1 ");
        }
        this.values = values;
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }
}