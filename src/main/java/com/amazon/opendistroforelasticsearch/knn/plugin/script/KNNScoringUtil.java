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

    public static float cosinesimil(float[] queryVector, float[] inputVector) {
        double dotProduct = 0.0f;
        double normQueryVector = 0.0f;
        double normInputVector = 0.0f;
        for (int i = 0; i < queryVector.length; i++) {
            dotProduct += queryVector[i] * inputVector[i];
            normQueryVector += queryVector[i] * queryVector[i];
            normInputVector += inputVector[i] * inputVector[i];
        }
        return (float) (dotProduct / (Math.sqrt(normQueryVector) * Math.sqrt(normInputVector)));
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

}
