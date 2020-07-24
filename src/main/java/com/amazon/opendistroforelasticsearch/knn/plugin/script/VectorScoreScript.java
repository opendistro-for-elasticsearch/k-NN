package com.amazon.opendistroforelasticsearch.knn.plugin.script;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class VectorScoreScript extends ScoreScript {

    private BinaryDocValues binaryEmbeddingReader;

    private final String field;
    private final boolean cosine;

    private final float[] inputVector;
    private final float magnitude;

    /**
     * This function called for each doc in the segment. We evaluate the score of the vector in the doc
     *
     * @param explanationHolder A helper to take in an explanation from a script and turn
     *                          it into an {@link org.apache.lucene.search.Explanation}
     * @return score of the vector to the query vector
     */
    @Override
    public double execute(ScoreScript.ExplanationHolder explanationHolder) {
        try {
            float[] doc_vector;
            BytesRef bytesref = binaryEmbeddingReader.binaryValue();
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(bytesref.bytes, bytesref.offset, bytesref.length);
                 ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
                doc_vector = (float[]) objectStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

//            float score = 0;
//            if (cosine) {
//                float docVectorNorm = 0.0f;
//                for (int i = 0; i < inputVector.length; i++) {
//                    float v = Float.intBitsToFloat(input.readInt());
//                    docVectorNorm += v * v;  // inputVector norm
//                    score += v * inputVector[i];  // dot product
//                }
//
//                if (docVectorNorm == 0 || magnitude == 0) {
//                    return 0f;
//                } else { // Convert cosine similarity range from (-1 to 1) to (0 to 1)
//                    return (1.0f + score / (Math.sqrt(docVectorNorm) * magnitude)) / 2.0f;
//                }
//            } else {
//                for (int i = 0; i < inputVector.length; i++) {
//                    float v = Float.intBitsToFloat(input.readInt());
//                    score += v * inputVector[i];  // dot product
//                }
//
//                return Math.exp(score); // Convert dot-proudct range from (-INF to +INF) to (0 to +INF)
//            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // again - Failing in order not to hide potential bugs
        }
        return 1.0;
    }

    @Override
    public void setDocument(int docId) {
        try {
            this.binaryEmbeddingReader.advanceExact(docId);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public VectorScoreScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);

        final Object cosineBool = params.get("cosine");
        this.cosine = cosineBool != null ?
                (boolean)cosineBool :
                true;

        final Object field = params.get("field");
        if (field == null)
            throw new IllegalArgumentException("binary_vector_score script requires field input");
        this.field = field.toString();

        // get query inputVector - convert to primitive
        final Object vector = params.get("vector");
        if(vector != null) {
            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
            inputVector = new float[tmp.size()];
            for (int i = 0; i < inputVector.length; i++) {
                inputVector[i] = tmp.get(i).floatValue();
            }
        } else {
            inputVector = null;
        }

        if (this.cosine) {
            // calc magnitude
            float queryVectorNorm = 0.0f;
            // compute query inputVector norm once
            for (float v: this.inputVector) {
                queryVectorNorm += v * v;
            }
            this.magnitude = (float) Math.sqrt(queryVectorNorm);
        } else {
            this.magnitude = 0.0f;
        }

        try {
            this.binaryEmbeddingReader = leafContext.reader().getBinaryDocValues(this.field);
            if(this.binaryEmbeddingReader == null) {
                throw new IllegalStateException();
            }
        } catch (Exception e) {
            throw new IllegalStateException("binaryEmbeddingReader can't be null, is '" + this.field +
                    "' the right binary vector field name, if so, is it defined as a binary type in the index mapping?");
        }

    }

    public static class VectorScoreScriptFactory implements ScoreScript.LeafFactory {
        private final Map<String, Object> params;
        private final SearchLookup lookup;

        public VectorScoreScriptFactory(Map<String, Object> params, SearchLookup lookup) {
            this.params = params;
            this.lookup = lookup;
        }

        public boolean needs_score() {
            return false;
        }

        @Override // called number of segments times
        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
            return new VectorScoreScript(this.params, this.lookup, ctx);
        }
    }
}

