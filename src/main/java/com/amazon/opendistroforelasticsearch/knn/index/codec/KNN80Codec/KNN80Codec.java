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

/**
 * Extends the Codec to support a new file format for KNN index
 * based on the mappings.
 *
 */
public final class KNN80Codec extends Codec {

    private static final Logger logger = LogManager.getLogger(KNN80Codec.class);
    private final DocValuesFormat docValuesFormat;
    private final DocValuesFormat perFieldDocValuesFormat;
    private final CompoundFormat compoundFormat;
    private Codec lucene80Codec;

    public static final String KNN_80 = "KNN80Codec";
    public static final String LUCENE_80 = "Lucene80"; // Lucene Codec to be used

    public KNN80Codec() {
        super(KNN_80);
        this.docValuesFormat = new KNN80DocValuesFormat();
        this.perFieldDocValuesFormat = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        this.compoundFormat = new KNN80CompoundFormat();
    }

    /*
     * This function returns the Lucene80 Codec.
     */
    public Codec getDelegatee() {
        if (lucene80Codec == null)
            lucene80Codec = Codec.forName(LUCENE_80);
        return lucene80Codec;
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
}
