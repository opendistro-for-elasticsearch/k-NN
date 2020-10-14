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


package com.amazon.opendistroforelasticsearch.knn.plugin;

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNN86Codec.KNN86Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.index.codec.CodecService;

import static com.amazon.opendistroforelasticsearch.knn.index.codec.KNN86Codec.KNN86Codec.KNN_86;

/**
 * KNNCodecService to inject the right KNNCodec version
 */
class KNNCodecService extends CodecService {

    KNNCodecService() {
        super(null, null);
    }

    /**
     * If the index is of type KNN i.e index.knn = true, We always
     * return the KNN Codec
     *
     * @param name dummy name
     * @return KNN86Codec
     */
    @Override
    public Codec codec(String name) {
        Codec codec = Codec.forName(KNN_86);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    public void setPostingsFormat(PostingsFormat postingsFormat) {
        ((KNN86Codec)codec("")).setPostingsFormat(postingsFormat);
    }
}
