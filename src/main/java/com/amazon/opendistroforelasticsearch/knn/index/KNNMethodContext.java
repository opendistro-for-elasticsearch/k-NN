/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * KNNMethodContext will contain the information necessary to produce an index from a mapping. It will encompass all
 * parameters necessary to build the index.
 *
 * Based on faiss, we break an index into 5 parts
 *  1. A list of vector transform operations - Hold off for now
 *  2. The method of the index to be searched
 *  3. An optional course quantizer
 *  4. An encoding scheme
 *  5. A refinement step - hold off for now
 *
 * KNNMethod context will need an engine associated with it
 *
 * validation will need to be performed by engine
 *
 * also, engine should be able to produce input to jni
 *
 */
public class KNNMethodContext implements ToXContentFragment {
    public KNNMethodContext(KNNEngine knnEngine, SpaceTypes spaceTypes, ComponentContext methodComponent,
                             KNNMethodContext courseQuantizerContext, ComponentContext encodingScheme) {
        this.knnEngine = knnEngine;
        this.spaceTypes = spaceTypes;
        this.methodComponent = methodComponent;
        this.courseQuantizerContext = courseQuantizerContext;
        this.encodingScheme = encodingScheme;
    }

    private KNNEngine knnEngine;
    private SpaceTypes spaceTypes;
    private ComponentContext methodComponent;
    private KNNMethodContext courseQuantizerContext;
    private ComponentContext encodingScheme;

    public String getName() {
        return methodComponent.name;
    }

    public Map<String, Object> getParameters() {
        return methodComponent.parameters;
    }

    public KNNEngine getEngine() {
        return knnEngine;
    }

    public SpaceTypes getSpaceTypes() {
        return spaceTypes;
    }

    public KNNMethodContext getCourseQuantizer() {
        return courseQuantizerContext;
    }

    public ComponentContext getEncoding() {
        return encodingScheme;
    }

    /**
     * This function uses the knnEngine to validate that the method can be used with this engine
     *
     * @return whether method is valid
     */
    public boolean validate() {
        return knnEngine.validateMethod(this);
    }

    public String generateMethod() {
        return knnEngine.generateMethod(this);
    }

    //TODO: Today
    // (4) Add functionality for faiss to produce string from Context
    // (5) Add testing logic for code changes
    // (6) Submit PR to faiss branch
    public static KNNMethodContext parse(Object in, KNNEngine parentEngine, SpaceTypes parentSpace) throws IOException {
        if (in instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> methodMap = (Map<String, ?>) in;

            // Parse engine
            Object engine = methodMap.get("engine");
            KNNEngine knnEngine;

            if (parentEngine != null) {
                knnEngine = parentEngine;
            } else if (engine == null) {
                knnEngine = KNNEngine.DEFAULT;
            } else if (!(engine instanceof String)) {
                throw new MapperParsingException("'engine' must be a string");
            } else {
                knnEngine = KNNEngine.getEngine((String) engine);
            }

            // Parse space
            Object space = methodMap.get("space_type");
            SpaceTypes knnSpaceType;
            if (parentSpace != null) {
                knnSpaceType = parentSpace;
            } else if (!(space instanceof String)) {
                knnSpaceType = SpaceTypes.L2;
            } else {
                knnSpaceType = SpaceTypes.getSpace((String) space);
            }

            // Parse method
            ComponentContext knnMethod = ComponentContext.parse(in);

            // Parse courseQuantizer
            Object courseQuantizer = methodMap.get("course_quantizer");
            KNNMethodContext knnCourseQuantizer = null;

            if (courseQuantizer != null) {
                knnCourseQuantizer = parse(methodMap.get("course_quantizer"), knnEngine, knnSpaceType);
            }

            // Parse encoding scheme
            Object encoder = methodMap.get("encoder");
            ComponentContext knnEncoder = null;

            if (encoder != null) {
                knnEncoder = ComponentContext.parse(encoder);
            }

            return new KNNMethodContext(knnEngine, knnSpaceType, knnMethod, knnCourseQuantizer, knnEncoder);
        }

        throw new MapperParsingException("Unable to parse mapping");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("engine", knnEngine.getKnnEngineName());
        builder.field("space_type", spaceTypes.getValue());
        builder = methodComponent.toXContent(builder, params);

        if (courseQuantizerContext != null) {
            builder.startObject("course_quantizer");
            builder = courseQuantizerContext.toXContent(builder, params);
            builder.endObject();
        }

        if (encodingScheme != null) {
            builder.startObject("encoder");
            builder = encodingScheme.toXContent(builder, params);
            builder.endObject();
        }

        return builder;
    }

    public static class ComponentContext implements ToXContentFragment {
        public ComponentContext(String name, Map<String, Object> parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        private String name;
        private Map<String, Object> parameters;

        public static ComponentContext parse(Object in) {
            if (in instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, ?> methodMap = (Map<String, ?>) in;

                // get name
                Object name = methodMap.get("name");

                if (!(name instanceof String)) {
                    throw new MapperParsingException("Unable to parse mapping 1");
                }
                String knnName = (String) name;

                // get parameters
                Object parameters = methodMap.get("parameters");
                Map<String, Object> knnParameters;

                if (parameters == null) {
                    knnParameters = null;
                } else if (parameters instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> knnParameters1 = Collections.checkedMap((Map<String, Object>) parameters, String.class, Object.class);

                    for (Object parameter : knnParameters1.values()) {
                        if (!(parameter instanceof String || parameter instanceof Integer)) {
                            throw new MapperParsingException("Invalid parameter type");
                        }
                    }

                    knnParameters = knnParameters1;
                } else {
                    throw new MapperParsingException("Unable to parse mapping 2  ");
                }

                return new ComponentContext(knnName, knnParameters);
            }
            throw new MapperParsingException("Unable to parse mapping 3");
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("name", name);
            builder.field("parameters", parameters);
            return builder;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getParameters() {
            return parameters;
        }
    }
}
