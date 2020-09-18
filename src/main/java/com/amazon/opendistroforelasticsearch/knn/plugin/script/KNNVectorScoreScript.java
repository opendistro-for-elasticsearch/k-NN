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
import java.util.Map;

/**
 * Vector score script used for adjusting the score based on similarity space
 * on a per document basis.
 *
 */
public class KNNVectorScoreScript extends ScoreScript {

    private BinaryDocValues binaryDocValuesReader;
    private final float[] queryVector;
    private final String similaritySpace;
    private float queryVectorSquaredMagnitude = -1;

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

            if(doc_vector.length != queryVector.length) {
                throw new IllegalStateException("[KNN] query vector and field vector dimensions mismatch. " +
                        "query vector: " + queryVector.length + ", stored vector: " + doc_vector.length);
            }

            if (KNNConstants.L2.equalsIgnoreCase(similaritySpace)) {
                score = KNNScoringUtil.l2Squared(this.queryVector, doc_vector);
                score = 1/(1 + score);
            } else if (KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace)) {
                // Scores cannot be negative so add +1 to the cosine score
                score = 1 + KNNScoringUtil.cosinesimilOptimized(this.queryVector, doc_vector, this.queryVectorSquaredMagnitude);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public KNNVectorScoreScript(Map<String, Object> params, String field, float[] queryVector, float queryVectorSquaredMagnitude,
                                String similaritySpace, SearchLookup lookup, LeafReaderContext leafContext) throws IOException {
        super(params, lookup, leafContext);
        // get query vector - convert to primitive
        final Object vector = params.get("vector");
        this.similaritySpace = similaritySpace;
        this.queryVector = queryVector;
        this.queryVectorSquaredMagnitude = queryVectorSquaredMagnitude;
        this.binaryDocValuesReader = leafContext.reader().getBinaryDocValues(field);
        if(this.binaryDocValuesReader == null) {
            throw new IllegalStateException("Binary Doc values not enabled for the field " + field
                                                        + " Please ensure the field type is knn_vector in mappings for this field");
        }
    }

    public static class VectorScoreScriptFactory implements ScoreScript.LeafFactory {
        private final Map<String, Object> params;
        private final SearchLookup lookup;
        private String similaritySpace;
        private String field;
        private final float[] qVector;
        private float qVectorSquaredMagnitude; // Used for cosine optimization

        public VectorScoreScriptFactory(Map<String, Object> params, SearchLookup lookup) {
            this.params = params;
            this.lookup = lookup;
            validateAndInitParams(params);

            // initialize
            this.qVector = KNNScoringUtil.convertVectorToPrimitive(params.get("vector"));
            // Optimization for cosinesimil
            if (KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace)) {
                // calculate the magnitude
                qVectorSquaredMagnitude = KNNScoringUtil.getVectorMagnitudeSquared(qVector);
            }
        }

        private void validateAndInitParams(Map<String, Object> params) {
            // query vector field
            final Object field = params.get("field");
            if (field == null)
                throw new IllegalArgumentException("Missing parameter [field]");
            this.field = field.toString();

            // query vector
            final Object qVector = params.get("vector");
            if (qVector == null) {
                throw new IllegalArgumentException("Missing query vector parameter [vector]");
            }

            // validate space
            final Object space = params.get("space");
            if (space == null) {
                throw new IllegalArgumentException("Missing parameter [space]");
            }
            this.similaritySpace = (String)space;
            if (!KNNConstants.COSINESIMIL.equalsIgnoreCase(similaritySpace) && !KNNConstants.L2.equalsIgnoreCase(similaritySpace)) {
                throw new IllegalArgumentException("Invalid space type. Please refer to the available space types.");
            }
        }

        public boolean needs_score() {
            return false;
        }

        @Override // called number of segments times
        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
            if (ctx.reader().getBinaryDocValues(this.field) == null) {
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
            return new KNNVectorScoreScript(this.params, this.field, this.qVector, this.qVectorSquaredMagnitude,
                    this.similaritySpace, this.lookup, ctx);
        }
    }
}
