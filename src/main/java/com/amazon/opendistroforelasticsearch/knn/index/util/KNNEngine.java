/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.index.util;

import com.amazon.opendistroforelasticsearch.knn.index.KNNMethod;
import com.amazon.opendistroforelasticsearch.knn.index.KNNMethodContext;
import com.amazon.opendistroforelasticsearch.knn.index.SpaceType;

import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.FAISS_NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NMSLIB_NAME;


/**
 * KNNEngine provides the functionality to validate and transform user defined indices into information that can be
 * passed to the respective k-NN library's JNI layer.
 */
public enum KNNEngine implements KNNLibrary {
    NMSLIB(NMSLIB_NAME, Nmslib.INSTANCE),
    FAISS(FAISS_NAME, Faiss.INSTANCE);

    public static final KNNEngine DEFAULT = NMSLIB;

    /**
     * Constructor for KNNEngine
     *
     * @param name name of engine
     * @param knnLibrary library the engine uses
     */
    KNNEngine(String name, KNNLibrary knnLibrary) {
        this.name = name;
        this.knnLibrary = knnLibrary;
    }

    private String name;
    private KNNLibrary knnLibrary;

    /**
     * Get the engine
     *
     * @param name of engine to be fetched
     * @return KNNEngine corresponding to name
     */
    public static KNNEngine getEngine(String name) {
        if(FAISS.getName().equalsIgnoreCase(name)) {
            return FAISS;
        }
        if (NMSLIB.getName().equalsIgnoreCase(name)){
            return NMSLIB;
        }
        throw new IllegalArgumentException("[KNN] Invalid engine type: " + name);
    }

    /**
     * Get the name of the engine
     *
     * @return name of the engine
     */
    public String getName() {
        return name;
    }

    @Override
    public String getLatestBuildVersion() {
        return knnLibrary.getLatestBuildVersion();
    }

    @Override
    public String getLatestLibVersion() {
        return knnLibrary.getLatestLibVersion();
    }

    @Override
    public String getExtension() {
        return knnLibrary.getExtension();
    }

    @Override
    public String getCompoundExtension() {
        return knnLibrary.getCompoundExtension();
    }

    @Override
    public KNNMethod getMethod(String methodName) {
        return knnLibrary.getMethod(methodName);
    }

    @Override
    public float score(float rawScore, SpaceType spaceType) {
        return knnLibrary.score(rawScore, spaceType);
    }

    @Override
    public void validateMethod(KNNMethodContext knnMethodContext) {
        knnLibrary.validateMethod(knnMethodContext);
    }

    @Override
    public String generateMethod(KNNMethodContext knnMethodContext) {
        return knnLibrary.generateMethod(knnMethodContext);
    }

    @Override
    public Map<String, Object> generateExtraParameterMap(KNNMethodContext knnMethodContext) {
        return knnLibrary.generateExtraParameterMap(knnMethodContext);
    }
}
