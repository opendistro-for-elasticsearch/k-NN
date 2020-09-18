package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class KNNScoringUtil {
    private static Logger logger = LogManager.getLogger(KNNScoringUtil.class);

    /**
     * This method calculates L2 squared distance between query vector
     * and input vector
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return L2 score
     */
    public static float l2Squared(float[] queryVector, float[] inputVector) {
        float squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i]-inputVector[i];
            squaredDistance += diff * diff;
        }
        return squaredDistance;
    }

    /**
     * This method can be used script to avoid repeated calculation of normalization
     * for query vector for each filtered documents
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @param normQueryVector normalized query vector value.
     * @return cosine score
     */
    public static float cosinesimilOptimized(float[] queryVector, float[] inputVector, float normQueryVector) {
        float dotProduct = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    /**
     * This method calculates cosine similarity
     *
     * @param queryVector query vector
     * @param inputVector input vector
     * @return cosine score
     */
    public static float cosinesimil(float[] queryVector, float[] inputVector) {
        float dotProduct = 0.0f;
        float normQueryVector = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normQueryVector += queryVector[i] * queryVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0 ) {
            logger.debug("Invalid vectors for cosine. Returning minimum score to put this result to end");
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    /**
     * Converts Object vector to primitive float[]
     *
     * @param vector input vector
     * @return Float array representing the vector
     */
    @SuppressWarnings("unchecked")
    public static float[] convertVectorToPrimitive(Object vector) {
        float[] primitiveVector = null;
        if (vector != null) {
            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
            primitiveVector = new float[tmp.size()];
            for (int i = 0; i < primitiveVector.length; i++) {
                primitiveVector[i] = tmp.get(i).floatValue();
            }
        }
        return primitiveVector;
    }

    /**
     * Calculates the magnitude of given vector
     *
     * @param inputVector input vector
     * @return Magnitude of vector
     */
    public static float getVectorMagnitudeSquared(float[] inputVector) {
        if (null == inputVector) {
            throw new IllegalStateException("vector magnitude cannot be evaluated as it is null");
        }
        float normInputVector = 0.0f;
        for (int i = 0; i < inputVector.length; i++) {
            normInputVector += inputVector[i] * inputVector[i];
        }
        return normInputVector;
    }
}
