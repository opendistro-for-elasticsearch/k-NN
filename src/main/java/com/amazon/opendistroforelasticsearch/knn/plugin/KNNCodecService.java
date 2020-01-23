package com.amazon.opendistroforelasticsearch.knn.plugin;

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNN80Codec;
import org.apache.lucene.codecs.Codec;
import org.elasticsearch.index.codec.CodecService;

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
     * @return KNN80Codec
     */
    @Override
    public Codec codec(String name) {
        Codec codec = Codec.forName(KNN80Codec.CODEC_NAME);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }
}
