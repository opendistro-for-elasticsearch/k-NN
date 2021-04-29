/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.KNN_ENGINE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.METHOD_HNSW;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.MINIMUM_DATAPOINTS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.SPACE_TYPE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.TRAINING_DATASET_SIZE_LIMIT;

/**
 * KNNMethodContext will contain the information necessary to produce a library index from an Elasticsearch mapping.
 * It will encompass all parameters necessary to build the index.
 *
 * We define 5 components that a k-NN library index can be made up of:
 * 1. KNNEngine is the underlying library that will be used to build/search the index (nmslib or faiss)
 * 2. SpaceType is the space that the index should be built with
 * 3. Main method component is the top level library index definition
 * 4. Coarse quantization component is the KNNMethodContext for the coarse quantizer
 * 5. Encoder defines how vectors should be encoded
 *
 */
public class KNNMethodContext implements ToXContentFragment {

    public static final Integer DEFAULT_TRAINING_DATASET_SIZE_LIMIT = 10000;
    public static final Integer DEFAULT_MINIMUM_DATAPOINTS = 100;

    public static final Integer MIN_TRAINING_DATASET_SIZE_LIMIT = 100;
    public static final Integer MIN_MINIMUM_DATAPOINTS = 0;

    public static final KNNMethodContext DEFAULT = new KNNMethodContext(KNNEngine.DEFAULT, SpaceType.DEFAULT,
                        new MethodComponentContext(METHOD_HNSW, Collections.emptyMap()), null, null,
    KNNMethodContext.MIN_TRAINING_DATASET_SIZE_LIMIT +1, KNNMethodContext.MIN_MINIMUM_DATAPOINTS + 1);

    private KNNEngine knnEngine;
    private SpaceType spaceType;
    private MethodComponentContext methodComponent;
    private KNNMethodContext coarseQuantizerContext;
    private MethodComponentContext encoder;

    private Integer trainingDatasetSizeLimit;
    private Integer minimumDatapoints;

    /**
     * Constructor
     *
     * @param knnEngine engine that this method uses
     * @param spaceType space type that this method uses
     * @param methodComponent MethodComponent describing the main index
     * @param coarseQuantizerContext Coarse quantizer context
     * @param encoder MethodComponent describing encoder
     * @param trainingDatasetSizeLimit Gets the maximum number of the points to be included in training phase
     * @param minimumDatapoints The minimum number of datapoints needed to build specialized index. If there are not enough datapoints, the
     *                          created segment will use the default representation
     */
    public KNNMethodContext(KNNEngine knnEngine, SpaceType spaceType, MethodComponentContext methodComponent,
                            KNNMethodContext coarseQuantizerContext, MethodComponentContext encoder,
                            Integer trainingDatasetSizeLimit, Integer minimumDatapoints) {
        this.knnEngine = knnEngine;
        this.spaceType = spaceType;
        this.methodComponent = methodComponent;
        this.coarseQuantizerContext = coarseQuantizerContext;
        this.encoder = encoder;
        this.trainingDatasetSizeLimit = trainingDatasetSizeLimit;
        this.minimumDatapoints = minimumDatapoints;
    }

    /**
     * Gets the main method component
     *
     * @return methodComponent
     */
    public MethodComponentContext getMethodComponent() {
        return methodComponent;
    }

    /**
     * Gets the engine to be used for this context
     *
     * @return knnEngine
     */
    public KNNEngine getEngine() {
        return knnEngine;
    }

    /**
     * Gets the space type for this context
     *
     * @return spaceType
     */
    public SpaceType getSpaceType() {
        return spaceType;
    }

    /**
     * Gets the quantizer context for this component
     *
     * @return coarseQuantizerContext
     */
    public KNNMethodContext getCoarseQuantizer() {
        return coarseQuantizerContext;
    }

    /**
     * Gets the encoder comoponent for this context
     *
     * @return encoder
     */
    public MethodComponentContext getEncoder() {
        return encoder;
    }

    /**
     * Gets the maximum number of the points to be included in training phase
     *
     * @return trainingDatasetSizeLimit
     */
    public Integer getTrainingDatasetSizeLimit() {
        return trainingDatasetSizeLimit;
    }

    /**
     * Gets the minimum number of datapoints needed to build specialized index. If there are not enough datapoints, the
     * created segment will use the default representation
     *
     * @return minimumDatapoints
     */
    public Integer getMinimumDatapoints() {
        return minimumDatapoints;
    }

    /**
     * This method uses the knnEngine to validate that the method is compatible with the engine
     *
     */
    public void validate() {
        knnEngine.validateMethod(this);
    }

    /**
     * This method generates the string describing the method to the jni layer. For some engines, this string will just
     * be the name of the method. For others, this string may contain parameters and be passed to an index factory. The
     * engine will take care of generating this string based on the context
     *
     * @return method string to be passed to engine library
     */
    public String generateMethod() {
        return knnEngine.generateMethod(this);
    }

    /**
     * Some engine libraries may require extra parameters to be explicitly set on the index. For example, for faiss, the
     * constructor for HNSW does not allow you to pass efConstruction or efSearch. To allow users to configure these
     * parameters for their library index, we need to build a map of "extra" parameters that will be passed to the
     * jni layer. In the jni layer, this map will be parsed. The key of the map will allow the jni layer to determine
     * what to do with the value.
     *
     * Further, in order to pass this map to the Codec's docValueConsumer, we need to encode it as a string. This is
     * because Lucene's fieldType's attributes is a map String, String. So, we use the engine to build the map and use
     * XContent factory to encode it as a string. The engine is in charge of determining which parameters should be
     * considered "extra".
     *
     * @return Json string representing the Map String, Object containing extra parameters
     */
    public String generateExtraParameters() {
        try {
            return Strings.toString(XContentFactory.jsonBuilder().map(knnEngine.generateExtraParameterMap(this)));
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to generate xcontent string for extra parameter map: " + ioe);
        }
    }

    /**
     * Parses an Object into a KNNMethodContext. This can be called recursively for coarse quantizers
     *
     * @param in Object containing mapping to be parsed
     * @param parentEngine engine type of parent. Relevant for recursion with coarse quantizers. Coarse quantizers must
     *                     have the same engine type as their parent index
     * @param parentSpace spaceType of parent index. Relevant for recursion with coarse quantizers. For top level index,
     *                    this can be null
     * @return KNNMethodContext
     */
    public static KNNMethodContext parse(Object in, KNNEngine parentEngine, SpaceType parentSpace) {
        if (!(in instanceof Map)) {
            throw new MapperParsingException("Unable to parse mapping into KNNMethodContext. Object not of type \"Map\"");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> methodMap = (Map<String, Object>) in;

        KNNEngine engine = parentEngine == null ? KNNEngine.DEFAULT : parentEngine;
        SpaceType spaceType = parentSpace == null ? SpaceType.L2 : parentSpace;
        String name = "";
        Map<String, Object> parameters = null;
        Object coarseQuantizerValue = null;
        MethodComponentContext encoder = null;

        Integer trainingDatasetSizeLimit = DEFAULT_TRAINING_DATASET_SIZE_LIMIT;
        Integer minimumDatapoints = DEFAULT_MINIMUM_DATAPOINTS;

        String key;
        Object value;
        for (Map.Entry<String, Object> methodEntry : methodMap.entrySet()) {
            key = methodEntry.getKey();
            value = methodEntry.getValue();
            if (KNN_ENGINE.equals(key)) {
                if (parentEngine != null && !parentEngine.getName().equals(value)) {
                    throw new MapperParsingException("Cannot set " + KNN_ENGINE + " different than parent "
                            + KNN_ENGINE);
                }

                if (value != null && !(value instanceof String)) {
                    throw new MapperParsingException("\"" + KNN_ENGINE + "\" must be a string");
                }

                if (value != null) {
                    try {
                        engine = KNNEngine.getEngine((String) value);
                    } catch (IllegalArgumentException iae) {
                        throw new MapperParsingException("Invalid " + KNN_ENGINE + ": " + value);
                    }
                }
            } else if (SPACE_TYPE.equals(key)) {
                if (parentSpace != null && !spaceType.getValue().equals(value)) {
                    throw new MapperParsingException("Cannot set " + SPACE_TYPE + " different than parent "
                            + SPACE_TYPE);
                }

                if (value != null && !(value instanceof String)) {
                    throw new MapperParsingException("\"" + SPACE_TYPE + "\" must be a string");
                }

                try {
                    spaceType = SpaceType.getSpace((String) value);
                } catch (IllegalArgumentException iae) {
                    throw new MapperParsingException("Invalid " + SPACE_TYPE + ": " + value);
                }
            } else if (NAME.equals(key)) {
                if (!(value instanceof String)) {
                    throw new MapperParsingException(NAME + "has to be a string");
                }

                name = (String) value;
            } else if (PARAMETERS.equals(key)) {
                if (value != null && !(value instanceof Map)) {
                    throw new MapperParsingException("Unable to parse parameters for main method component");
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> parameters1 = (Map<String, Object>) value;
                parameters = parameters1;
            } else if (COARSE_QUANTIZER.equals(key)) {
                // Cannot parse coarse quantizer until the end because the engine and space may not be set
                coarseQuantizerValue = value;
            } else if (ENCODER.equals(key)) {
                encoder = MethodComponentContext.parse(value);
            } else if (TRAINING_DATASET_SIZE_LIMIT.equals(key)) {
                if (!(value instanceof Integer)) {
                    throw new MapperParsingException(TRAINING_DATASET_SIZE_LIMIT + "has to be a Integer");
                }

                if ((Integer) value < MIN_TRAINING_DATASET_SIZE_LIMIT) {
                    throw new MapperParsingException(TRAINING_DATASET_SIZE_LIMIT + "has to be equal to or greater than 100");
                }

                trainingDatasetSizeLimit = (Integer) value;
            } else if (MINIMUM_DATAPOINTS.equals(key)) {
                if (!(value instanceof Integer)) {
                    throw new MapperParsingException(MINIMUM_DATAPOINTS + "has to be a Integer");
                }

                if ((Integer) value < MIN_MINIMUM_DATAPOINTS) {
                    throw new MapperParsingException(MINIMUM_DATAPOINTS + "has to be equal to or greater than 0");
                }

                minimumDatapoints = (Integer) value;
            } else {
                throw new MapperParsingException("Invalid parameter: " + key);
            }
        }

        if (name.isEmpty()) {
            throw new MapperParsingException(NAME + " needs to be set");
        }

        MethodComponentContext method = new MethodComponentContext(name, parameters);

        KNNMethodContext coarseQuantizer = null;
        if (coarseQuantizerValue != null) {
            coarseQuantizer = parse(coarseQuantizerValue, engine, spaceType);
        }

        return new KNNMethodContext(engine, spaceType, method, coarseQuantizer, encoder, trainingDatasetSizeLimit,
                minimumDatapoints);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KNN_ENGINE, knnEngine.getName());
        builder.field(SPACE_TYPE, spaceType.getValue());
        builder.field(TRAINING_DATASET_SIZE_LIMIT, trainingDatasetSizeLimit);
        builder.field(MINIMUM_DATAPOINTS, minimumDatapoints);
        builder = methodComponent.toXContent(builder, params);

        if (coarseQuantizerContext != null) {
            builder.startObject(COARSE_QUANTIZER);
            builder = coarseQuantizerContext.toXContent(builder, params);
            builder.endObject();
        }

        if (encoder != null) {
            builder.startObject(ENCODER);
            builder = encoder.toXContent(builder, params);
            builder.endObject();
        }

        return builder;
    }
}
