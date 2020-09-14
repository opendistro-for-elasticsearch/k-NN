package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class KNNScoringUtil {
    private static Logger logger = LogManager.getLogger(KNNScoringUtil.class);

    public static float l2Squared(float[] queryVector, float[] inputVector) {
        float squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            float diff = queryVector[i]-inputVector[i];
            squaredDistance += diff * diff;
        }
        return squaredDistance;
    }

    public static float cosinesimilOptimized(float[] queryVector, float[] inputVector, float normQueryVector) {
        float dotProduct = 0.0f;
        float normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        float normalizedProduct = normQueryVector * normInputVector;
        try {
            return (float) (dotProduct / (Math.sqrt(normalizedProduct)));
        } catch(ArithmeticException ex) {
            logger.debug("Possibly Division by Zero Exception. Returning min score to put this result to end. " +
                    "Current normalized product " + normalizedProduct);
            return Float.MIN_VALUE;
        }
    }

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
