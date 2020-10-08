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

package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.plugin.stats.KNNCounter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Map;

/**
 * Binary score script used for adjusting the score based on similarity space
 * on a per document basis.
 *
 */
public class KNNBinaryScoreScript extends ScoreScript {

    private BinaryDocValues binaryDocValuesReader;
    private final String queryBase64;
    private final String similaritySpace;
    private boolean binaryExist = true;

    private static final Logger logger = LogManager.getLogger(KNNBinaryScoreScript.class);

    /**
     * This function called for each doc in the segment. We evaluate the score of the vector in the doc
     *
     * @param explanationHolder A helper to take in an explanation from a script and turn
     *                          it into an {@link org.apache.lucene.search.Explanation}
     * @return score of the vector to the query vector
     */
    @Override
    public double execute(ScoreScript.ExplanationHolder explanationHolder) {
        // If this document does not contain the vector, push it to end of the results.
        if (!binaryExist) {
            return Float.MIN_VALUE;
        }

        float score = Float.MIN_VALUE;
        try {
            String docBase64;
            BytesRef bytesref = binaryDocValuesReader.binaryValue();

            if (bytesref == null) {
                return Float.MIN_VALUE;
            }
            try  {
                docBase64 = Base64.getEncoder().encodeToString(bytesref.bytes);
                logger.info(docBase64);
            } catch (Exception e) {
                KNNCounter.SCRIPT_QUERY_ERRORS.increment();
                throw new RuntimeException(e);
            }

            if (KNNConstants.BIT_HAMMING.equalsIgnoreCase(similaritySpace)) {
                score = KNNScoringUtil.hamming(this.queryBase64, docBase64);
                score = 1/(1 + score);
            }
        } catch (IOException e) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new UncheckedIOException(e);
        }
        return score;
    }

    @Override
    public void setDocument(int docId) {
        try {
            this.binaryExist = this.binaryDocValuesReader.advanceExact(docId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public KNNBinaryScoreScript(Map<String, Object> params, String field, String queryBase64, String similaritySpace,
                                SearchLookup lookup, LeafReaderContext leafContext) throws IOException {
        super(params, lookup, leafContext);
        this.similaritySpace = similaritySpace;
        this.queryBase64 = queryBase64;
        this.binaryDocValuesReader = leafContext.reader().getBinaryDocValues(field);
        if(this.binaryDocValuesReader == null) {
            KNNCounter.SCRIPT_QUERY_ERRORS.increment();
            throw new IllegalStateException("Terms not enabled for the field " + field
                                                        + " Please ensure the field type is knn_vector in mappings for this field");
        }
    }
}
