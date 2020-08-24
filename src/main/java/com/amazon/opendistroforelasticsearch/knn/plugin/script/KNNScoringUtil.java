package com.amazon.opendistroforelasticsearch.knn.plugin.script;

public class KNNScoringUtil {

    public static float l2Squared(float[] queryVector, float[] inputVector) {
        long squaredDistance = 0;
        for (int i = 0; i < inputVector.length; i++) {
            squaredDistance += Math.pow(queryVector[i]-inputVector[i], 2);
        }
        return squaredDistance;
    }

}
