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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public final class KNNVectorScriptDocValues extends ScriptDocValues<float[]> {

    private final BinaryDocValues binaryDocValues;
    private final String fieldName;
    private boolean docExists;

    public KNNVectorScriptDocValues(BinaryDocValues binaryDocValues, String fieldName) {
        this.binaryDocValues = binaryDocValues;
        this.fieldName = fieldName;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (binaryDocValues.advanceExact(docId)) {
            docExists = true;
            return;
        }
        docExists = false;
    }

    public float[] getValue() {
        if (!docExists) {
            String errorMessage = String.format(
                "One of the document doesn't have a value for field '%s'. " +
                "This can be avoided by checking if a document has a value for the field or not " +
                "by doc['%s'].size() == 0 ? 0 : {your script}",fieldName,fieldName);
            throw new IllegalStateException(errorMessage);
        }
        try {
            BytesRef value = binaryDocValues.binaryValue();
            ByteArrayInputStream byteStream = new ByteArrayInputStream(value.bytes, value.offset, value.length);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);
            return (float[]) objectStream.readObject();
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException((e));
        }
    }

    @Override
    public int size() {
        return docExists ? 1 : 0;
    }

    @Override
    public float[] get(int i) {
        throw new UnsupportedOperationException("knn vector does not support this operation");
    }
}
