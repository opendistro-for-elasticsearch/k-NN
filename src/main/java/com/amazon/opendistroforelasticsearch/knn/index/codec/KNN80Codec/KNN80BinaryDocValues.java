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

package com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec;

import com.amazon.opendistroforelasticsearch.knn.index.codec.BinaryDocValuesSub;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * A per-document kNN numeric value.
 */
class KNN80BinaryDocValues extends BinaryDocValues {

    private DocIDMerger<BinaryDocValuesSub> docIDMerger;

    KNN80BinaryDocValues(DocIDMerger<BinaryDocValuesSub> docIdMerger) {
        this.docIDMerger = docIdMerger;
    }

    private BinaryDocValuesSub current;
    private int docID = -1;

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
            docID = NO_MORE_DOCS;
        } else {
            docID = current.mappedDocID;
        }
        return docID;
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return current.getValues().binaryValue();
    }
};
