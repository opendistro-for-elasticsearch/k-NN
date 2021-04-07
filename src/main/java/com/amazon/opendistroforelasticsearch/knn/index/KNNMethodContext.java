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
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.COARSE_QUANTIZER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.ENCODER;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.KNN_ENGINE;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.NAME;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.PARAMETERS;
import static com.amazon.opendistroforelasticsearch.knn.common.KNNConstants.SPACE_TYPE;

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
    public KNNMethodContext(KNNEngine knnEngine, SpaceType spaceType, MethodComponentContext methodComponent,
                            KNNMethodContext coarseQuantizerContext, MethodComponentContext encoder) {
        this.knnEngine = knnEngine;
        this.spaceType = spaceType;
        this.methodComponent = methodComponent;
        this.coarseQuantizerContext = coarseQuantizerContext;
        this.encoder = encoder;
    }

    private KNNEngine knnEngine;
    private SpaceType spaceType;
    private MethodComponentContext methodComponent;
    private KNNMethodContext coarseQuantizerContext;
    private MethodComponentContext encoder;

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
        if (in instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> methodMap = (Map<String, Object>) in;

            Object engine = methodMap.get(KNN_ENGINE);
            KNNEngine knnEngine;
            if (parentEngine != null) {
                knnEngine = parentEngine;
            } else if (engine == null) {
                knnEngine = KNNEngine.DEFAULT;
            } else if (!(engine instanceof String)) {
                throw new MapperParsingException("\"" + KNN_ENGINE + "\" must be a string");
            } else {
                knnEngine = KNNEngine.getEngine((String) engine);
            }

            Object space = methodMap.get(SPACE_TYPE);
            SpaceType knnSpaceType;
            if (parentSpace != null) {
                knnSpaceType = parentSpace;
            } else if (space == null) {
                knnSpaceType = SpaceType.L2;
            } else if (!(space instanceof String)) {
                throw new MapperParsingException("\"" + SPACE_TYPE + "\" must be a string");
            } else {
                knnSpaceType = SpaceType.getSpace((String) space);
            }

            MethodComponentContext knnMethod = MethodComponentContext.parse(in);

            Object coarseQuantizer = methodMap.get(COARSE_QUANTIZER);
            KNNMethodContext knnCoarseQuantizer = null;

            if (coarseQuantizer != null) {
                knnCoarseQuantizer = parse(coarseQuantizer, knnEngine, knnSpaceType);
            }

            Object encoder = methodMap.get(ENCODER);
            MethodComponentContext knnEncoder = null;

            if (encoder != null) {
                knnEncoder = MethodComponentContext.parse(encoder);
            }

            return new KNNMethodContext(knnEngine, knnSpaceType, knnMethod, knnCoarseQuantizer, knnEncoder);
        }

        throw new MapperParsingException("Unable to parse mapping into KNNMethodContext. Object not of type \"Map\"");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KNN_ENGINE, knnEngine.getName());
        builder.field(SPACE_TYPE, spaceType.getValue());
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

    /**
     * MethodComponentContext represents a single building block of a knn library index.
     *
     * Each component is composed of a name and a map of parameters.
     */
    public static class MethodComponentContext implements ToXContentFragment {
        public MethodComponentContext(String name, Map<String, Object> parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        private String name;
        private Map<String, Object> parameters;

        /**
         * Parses the object into MethodComponentContext
         *
         * @param in Object to be parsed
         * @return MethodComponentContext
         */
        public static MethodComponentContext parse(Object in) {
            if (in instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> methodMap = (Map<String, Object>) in;

                Object name = methodMap.get(NAME);

                if (name == null) {
                    throw new MapperParsingException("Component name needs to be set");
                } else if (!(name instanceof String)) {
                    throw new MapperParsingException("Component name should be a string");
                }
                String knnName = (String) name;

                Object parameters = methodMap.get(PARAMETERS);
                Map<String, Object> knnParameters;

                if (parameters == null) {
                    knnParameters = null;
                } else if (parameters instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> knnParameters1 = (Map<String, Object>) parameters;

                    for (Object parameter : knnParameters1.values()) {
                        if (!(parameter instanceof String || parameter instanceof Integer)) {
                            throw new MapperParsingException("Invalid parameter type");
                        }
                    }

                    knnParameters = knnParameters1;
                } else {
                    throw new MapperParsingException("Unable to parse parameters of MethodComponent: " + name);
                }

                return new MethodComponentContext(knnName, knnParameters);
            }
            throw new MapperParsingException("Unable to parse MethodComponent");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME, name);
            builder.field(PARAMETERS, parameters);
            return builder;
        }

        /**
         * Gets the name of the component
         *
         * @return name
         */
        public String getName() {
            return name;
        }

        /**
         * Gets the parameters of the component
         *
         * @return parameters
         */
        public Map<String, Object> getParameters() {
            return parameters;
        }
    }
}
