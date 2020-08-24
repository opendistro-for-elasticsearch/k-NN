package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Vector score script used for adjusting the score based on similarity space
 * on a per document basis.
 */
public class VectorScoreScript extends ScoreScript {

    private BinaryDocValues binaryDocValuesReader;
    private final float[] inputVector;
    private final String similaritySpace;

    public float l2Squared(float[] queryVector, float[] inputVector) {
        long squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            squaredDistance += Math.pow(queryVector[i]-inputVector[i], 2);
        }
        return squaredDistance;
    }

    /**
     * This function called for each doc in the segment. We evaluate the score of the vector in the doc
     *
     * @param explanationHolder A helper to take in an explanation from a script and turn
     *                          it into an {@link org.apache.lucene.search.Explanation}
     * @return score of the vector to the query vector
     */
    @Override
    public double execute(ScoreScript.ExplanationHolder explanationHolder) {
        float score = Float.MIN_VALUE;
        try {
            float[] doc_vector;
            BytesRef bytesref = binaryDocValuesReader.binaryValue();
            // If there is no vector for the corresponding doc then it should be not considered for nearest
            // neighbors.
            if (bytesref == null) {
                return Float.MIN_VALUE;
            }
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytesref.bytes, bytesref.offset, bytesref.length);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                doc_vector = (float[]) objectStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (KNNConstants.L2.equalsIgnoreCase(similaritySpace)) {
                score = l2Squared(this.inputVector, doc_vector);
                score = 1/(1 + score);
            }
            // Other spaces will be followed up in next pr
        } catch (IOException e) {
            throw new UncheckedIOException(e); // again - Failing in order not to hide potential bugs
        }
        return score;
    }

    @Override
    public void setDocument(int docId) {
        try {
            this.binaryDocValuesReader.advanceExact(docId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public VectorScoreScript(Map<String, Object> params, String field, String similaritySpace,
                             SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        // get query inputVector - convert to primitive
        final Object vector = params.get("vector");
        this.similaritySpace = similaritySpace;
        if(vector != null) {
            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
            inputVector = new float[tmp.size()];
            for (int i = 0; i < inputVector.length; i++) {
                inputVector[i] = tmp.get(i).floatValue();
            }
        } else {
            inputVector = null;
        }

        try {
            this.binaryDocValuesReader = leafContext.reader().getBinaryDocValues(field);
            if(this.binaryDocValuesReader == null) {
                throw new IllegalStateException();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Binary Doc values not enabled for the field " + field
                    + " Please ensure the field type is knn_vector in mappings for this field");
        }
    }

    public static class VectorScoreScriptFactory implements ScoreScript.LeafFactory {
        private final Map<String, Object> params;
        private final SearchLookup lookup;
        private final String similaritySpace;
        private final String field;

        public VectorScoreScriptFactory(Map<String, Object> params, SearchLookup lookup) {
            this.params = params;
            this.lookup = lookup;

            params.containsKey("");
            final Object field = params.get("field");
            if (field == null)
                throw new IllegalArgumentException("Missing parameter [field]");
            this.field = field.toString();

            final Object space = params.get("space");
            this.similaritySpace = space != null? (String)space: KNNConstants.L2;

            if (params.get("") ) {

            }
        }

        public boolean needs_score() {
            return false;
        }

        @Override // called number of segments times
        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
            if (ctx.reader().getBinaryDocValues("field") == null) {
                /*
                 * the field and/or term don't exist in this segment,
                 * so always return 0
                 */
                return new ScoreScript(params, lookup, ctx) {
                    @Override
                    public double execute(
                            ExplanationHolder explanation
                    ) {
                        return 0.0d;
                    }
                };
            }
            return new VectorScoreScript(this.params, this.field, this.similaritySpace, this.lookup, ctx);
        }
    }
}

