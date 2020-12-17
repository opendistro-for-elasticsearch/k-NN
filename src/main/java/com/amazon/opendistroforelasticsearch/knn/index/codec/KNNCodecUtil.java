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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class KNNCodecUtil {

    public static final String HNSW_EXTENSION = ".hnsw";
    public static final String HNSW_COMPOUND_EXTENSION = ".hnswc";

    public static final class Pair {
        public Pair(int[] docs, float[][] vectors) {
            this.docs = docs;
            this.vectors = vectors;
            vectorsStr = new String[0];
        }
        public Pair(int[] docs, String[] vectorsStr) {
            this.docs = docs;
            this.vectors = new float[0][0];
            this.vectorsStr = vectorsStr;
        }
        public int[] docs;
        public float[][] vectors;
        public String[] vectorsStr;
    }

    public static KNNCodecUtil.Pair getFloats(BinaryDocValues values) throws IOException {
        ArrayList<float[]> vectorList = new ArrayList<>();
        ArrayList<Integer> docIdList = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            BytesRef bytesref = values.binaryValue();
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytesref.bytes, bytesref.offset, bytesref.length);
                ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                float[] vector = (float[]) objectStream.readObject();
                vectorList.add(vector);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            docIdList.add(doc);
        }
        return new KNNCodecUtil.Pair(docIdList.stream().mapToInt(Integer::intValue).toArray(), vectorList.toArray(new float[][]{}));
    }

    public static KNNCodecUtil.Pair getStrings(BinaryDocValues values) throws IOException {
        ArrayList<String> vectorList = new ArrayList<>();
        ArrayList<Integer> docIdList = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            BytesRef bytesref = values.binaryValue();
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytesref.bytes, bytesref.offset, bytesref.length);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                float[] vector = (float[]) objectStream.readObject();
                //[1,0,1,0,1] --> "1 0 1 0 1"
                char[] charArray = new char[vector.length*2];
                for (int bit = 0; bit < vector.length; ++bit) {
                    int oneBit = ((int)(vector[bit]))%2;
                    charArray[bit*2] = (char) ('0' + oneBit);
                    charArray[bit*2+1] = ' ';
                }
                vectorList.add(String.valueOf(charArray));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            docIdList.add(doc);
        }
        return new KNNCodecUtil.Pair(docIdList.stream().mapToInt(Integer::intValue).toArray(), vectorList.toArray(new String[]{}));
    }
}
