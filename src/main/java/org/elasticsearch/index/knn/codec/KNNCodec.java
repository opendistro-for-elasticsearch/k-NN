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

package org.elasticsearch.index.knn.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

/**
 * Extends the Codec to support a new file format for KNN index
 * based on the mappings.
 *
 */
public final class KNNCodec extends Codec {

    private static final Logger logger = LogManager.getLogger(KNNCodec.class);
    private static final String KNN_CODEC = "KNNCodec";

    public static final String HNSW_EXTENSION = ".hnsw";
    public static final String HNSW_COMPOUND_EXTENSION = ".hnswc";

    private final DocValuesFormat docValuesFormat;
    private final DocValuesFormat perFieldDocValuesFormat;
    private final CompoundFormat compoundFormat;

    public KNNCodec() {
        super(KNN_CODEC);
        this.docValuesFormat = new KNNDocValuesFormat();
        this.perFieldDocValuesFormat = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        this.compoundFormat = new KNNCompoundFormat();
    }

    /*
     * This function returns the latest Codec supported by current ES version.
     */
    public Codec getDelegatee() {
        return Codec.getDefault();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return this.perFieldDocValuesFormat;
    }

    /*
     * For all the below functions, we could have extended FilterCodec, but this brings
     * SPI related issues while loading Codec in the tests. So fall back to traditional
     * approach of manually overriding.
     */

    @Override
    public PostingsFormat postingsFormat() {
        return getDelegatee().postingsFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return getDelegatee().storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return getDelegatee().termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return getDelegatee().fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return getDelegatee().segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return getDelegatee().normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return getDelegatee().liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return this.compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return getDelegatee().pointsFormat();
    }

    public static final class Pair {
        public Pair(int[] docs, float[][] vectors) {
            this.docs = docs;
            this.vectors = vectors;
        }

        public int[] docs;
        public float[][] vectors;
    }

    public static KNNCodec.Pair getFloats(BinaryDocValues values) throws IOException {
        ArrayList<float[]> vectorList = new ArrayList<>();
        ArrayList<Integer> docIdList = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            byte[] value = values.binaryValue().bytes;

            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(value);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                float[] vector = (float[]) objectStream.readObject();
                vectorList.add(vector);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            docIdList.add(doc);
        }
        return new KNNCodec.Pair(docIdList.stream().mapToInt(Integer::intValue).toArray(), vectorList.toArray(new float[][]{}));
    }
}
