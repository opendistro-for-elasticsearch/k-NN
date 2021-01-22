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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

public class KNNVectorDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String fieldName;

    public KNNVectorDVLeafFieldData(LeafReader reader, String fieldName) {
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }

    @Override
    public ScriptDocValues<float[]> getScriptValues() {
        try {
            BinaryDocValues values = DocValues.getBinary(reader, fieldName);
            return new KNNVectorScriptDocValues(values, fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for knn vector field: "+fieldName, e);
        }
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("knn vector field '"+ fieldName + "' doesn't support sorting");
    }
}
