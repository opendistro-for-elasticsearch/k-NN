package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import java.util.ArrayList;

public class KNNScoringUtil {

    public static float l2Squared(float[] queryVector, float[] inputVector) {
        float squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i]-inputVector[i];
            squaredDistance += diff * diff;
        }
        return squaredDistance;
    }

    public static float cosinesimilOptimized(float[] queryVector, float[] inputVector, double normQueryVector) {
        double dotProduct = 0.0f;
        double normInputVector = 0.0f;
        if (normQueryVector == -1) {
            throw new IllegalStateException("Normalized query vector cannot be negative");
        }
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        // Divide by zero check
        double normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0 ) {
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }


    public static float cosinesimil(float[] queryVector, float[] inputVector) {
        double dotProduct = 0.0f;
        double normQueryVector = 0.0f;
        double normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normQueryVector += queryVector[i] * queryVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        double normalizedProduct = normQueryVector * normInputVector;
        if (normalizedProduct == 0 ) {
            return Float.MIN_VALUE;
        }
        return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
    }

    @SuppressWarnings("unchecked")
    public static float[] convertVectorToPrimitive(Object vector) {
        float[] primitiveVector = null;
        if(vector != null) {
            final ArrayList<Double> tmp = (ArrayList<Double>) vector;
            primitiveVector = new float[tmp.size()];
            for (int i = 0; i < primitiveVector.length; i++) {
                primitiveVector[i] = tmp.get(i).floatValue();
            }
        }
        return primitiveVector;
    }

    public static double getVectorMagnitudeSquared(float[] inputVector) {
        if (null == inputVector) {
            throw new IllegalStateException("vector magnitude cannot be evaluated as it is null");
        }
        double normInputVector = 0.0f;
        for (int i = 0; i < inputVector.length; i++) {
            normInputVector += inputVector[i] * inputVector[i];
        }
        return (float) normInputVector;
    }
}
