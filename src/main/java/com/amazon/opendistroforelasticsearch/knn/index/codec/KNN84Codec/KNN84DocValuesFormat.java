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

package com.amazon.opendistroforelasticsearch.knn.index.codec.KNN84Codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

import static com.amazon.opendistroforelasticsearch.knn.index.codec.KNN84Codec.KNN84Codec.LUCENE_DOC_VALUES_FORMAT;


/**
 * Encodes/Decodes per document values
 */
class KNN84DocValuesFormat extends DocValuesFormat {
    private final Logger logger = LogManager.getLogger(KNN84DocValuesFormat.class);
    private final DocValuesFormat delegate = DocValuesFormat.forName(LUCENE_DOC_VALUES_FORMAT);

    KNN84DocValuesFormat() {
        super(LUCENE_DOC_VALUES_FORMAT);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new KNN84DocValuesConsumer(delegate.fieldsConsumer(state), state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }
}
