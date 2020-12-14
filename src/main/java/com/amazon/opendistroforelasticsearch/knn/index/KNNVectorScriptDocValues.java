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

package com.amazon.opendistroforelasticsearch.knn.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;

// This class is thread safe, since docExists is synchronized at an instance level
public final class KNNVectorScriptDocValues extends ScriptDocValues<float[]> {

    private final BinaryDocValues binaryDocValues;
    private boolean docExists;

    public KNNVectorScriptDocValues(BinaryDocValues binaryDocValues) {
        this.binaryDocValues = binaryDocValues;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        synchronized (this) {
            if (binaryDocValues.advanceExact(docId)) {
                docExists = true;
                return;
            }
            docExists = false;
        }
    }

    public synchronized float[] getValue() throws IOException {
        if (!docExists) {
            throw new IllegalArgumentException("no value found for the corresponding doc ID");
        }
        BytesRef value = binaryDocValues.binaryValue();
        Objects.requireNonNull(value);
        ByteArrayInputStream byteStream = new ByteArrayInputStream(value.bytes, value.offset, value.length);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        try {
            return (float[]) objectStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException((e));
        }
    }

    @Override
    public int size() {
        synchronized (this) {
            return docExists ? 1 : 0;
        }
    }

    @Override
    public float[] get(int i) {
        throw new UnsupportedOperationException("knn vector does not support this operation");
    }
}
