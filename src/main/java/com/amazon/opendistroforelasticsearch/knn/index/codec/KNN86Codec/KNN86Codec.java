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

package com.amazon.opendistroforelasticsearch.knn.index.codec.KNN86Codec;

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec.KNN80CompoundFormat;
import com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec.KNN80DocValuesFormat;
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
public final class KNN86Codec extends Codec {

    private static final Logger logger = LogManager.getLogger(KNN86Codec.class);
    private final DocValuesFormat docValuesFormat;
    private final DocValuesFormat perFieldDocValuesFormat;
    private final CompoundFormat compoundFormat;
    private Codec lucene86Codec;
    private PostingsFormat postingsFormat = null;

    public static final String KNN_86 = "KNN86Codec";
    public static final String LUCENE_86 = "Lucene86"; // Lucene Codec to be used

    public KNN86Codec() {
        super(KNN_86);
        // Note that DocValuesFormat can use old Codec's DocValuesFormat. For instance Lucene84 uses Lucene80
        // DocValuesFormat. Refer to defaultDVFormat in LuceneXXCodec.java to find out which version it uses
        this.docValuesFormat =  new KNN80DocValuesFormat();
        this.perFieldDocValuesFormat = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        this.compoundFormat = new KNN80CompoundFormat();
    }

    /*
     * This function returns the Lucene84 Codec.
     */
    public Codec getDelegatee() {
        if (lucene86Codec == null)
            lucene86Codec = Codec.forName(LUCENE_86);
        return lucene86Codec;
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


    public void setPostingsFormat(PostingsFormat postingsFormat) {
        this.postingsFormat = postingsFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        if (this.postingsFormat == null) {
            return getDelegatee().postingsFormat();
        }
        return this.postingsFormat;
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
