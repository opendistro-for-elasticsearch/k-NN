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

package com.amazon.opendistroforelasticsearch.knn.common;

public class KNNConstants {
    public static final String SPACE_TYPE = "spaceType";
    public static final String HNSW_ALGO_M = "M";
    public static final String HNSW_ALGO_EF_CONSTRUCTION = "efConstruction";
    public static final String HNSW_ALGO_EF_SEARCH = "efSearch";
    public static final String HNSW_ALGO_INDEX_THREAD_QTY = "indexThreadQty";
    public static final String DIMENSION = "dimension";
    public static final String KNN_ENGINE = "engine";
    public static final String KNN_METHOD= "method";
    public static final String EXTRA_PARAMETERS = "extra_parameters";
    public static final String COARSE_QUANTIZER = "coarse_quantizer";
    public static final String ENCODER = "encoder";
    public static final String NAME = "name";
    public static final String PARAMETERS = "parameters";

    public static final String NMSLIB_NAME = "NMSLIB";
    public static final String FAISS_NAME = "FAISS";

    public static final String NMSLIB_EXTENSION = ".hnsw";
    public static final String FAISS_EXTENSION = ".faiss";

    public static final String METHOD_HNSW = "hnsw";
    public static final String METHOD_IVF = "ivf";

    public static final String METHOD_PARAMETER_EF_CONSTRUCTION = "ef_construction";
    public static final String METHOD_PARAMETER_EF_SEARCH = "ef_search";
    public static final String METHOD_PARAMETER_M = "m";
    public static final String METHOD_PARAMETER_NCENTROIDS = "ncentroids";
    public static final String METHOD_PARAMETER_NPROBES = "nprobes";
    public static final String METHOD_PARAMETER_CODE_SIZE = "code_size";
    public static final String TRAINING_DATASET_SIZE_LIMIT = "training_dataset_size_limit";
    public static final String MINIMUM_DATAPOINTS = "minimum_datapoints";

    public static final String ENCODER_PQ = "pq";
    public static final String ENCODER_FLAT = "flat";
}
