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

import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_ALGO_PARAM_M_SETTING;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_ALGO_PARAM_M;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_DEFAULT_SPACE_TYPE;
import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.INDEX_KNN_SPACE_TYPE;

/**
 * Field Mapper for KNN vector type.
 */
public class KNNVectorFieldMapper extends ParametrizedFieldMapper {

    private static Logger logger = LogManager.getLogger(KNNVectorFieldMapper.class);

    public static final String CONTENT_TYPE = "knn_vector";
    public static final String KNN_FIELD = "knn_field";

    static final int MAX_DIMENSION = 10000;

    private static KNNVectorFieldMapper toType(FieldMapper in) {
        return (KNNVectorFieldMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {
        protected Boolean ignoreMalformed;

        private final Parameter<Boolean> stored = Parameter.boolParam("store", false,
                m -> toType(m).stored, false);
        private final Parameter<Boolean> hasDocValues = Parameter.boolParam("doc_values", false,
                m -> toType(m).hasDocValues,  true);
        private final Parameter<Integer> dimension = new Parameter<>(KNNConstants.DIMENSION, false, -1,
                (n, o) -> {
                            int value = XContentMapValues.nodeIntegerValue(o);
                            if (value > MAX_DIMENSION) {
                                throw new IllegalArgumentException("Dimension value cannot be greater than " +
                                    MAX_DIMENSION + " for vector: " + name);
                            }

                            if (value <= 0) {
                                throw new IllegalArgumentException("Dimension value must be greater than 0 " +
                                        "for vector: " + name);
                            }
                            return value;
                }, m -> toType(m).dimension);

        private final Parameter<Map<String, String>> meta = new Parameter<>("meta", true,
                Collections.emptyMap(), TypeParsers::parseMeta, m -> m.fieldType().meta());

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(stored, hasDocValues, dimension, meta);
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
            }
            return KNNVectorFieldMapper.Defaults.IGNORE_MALFORMED;
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {
            return new KNNVectorFieldMapper(name, new KNNVectorFieldType(buildFullName(context), meta.getValue(),
                    dimension.getValue()), multiFieldsBuilder.build(this, context),
                    ignoreMalformed(context), getSpaceType(context.indexSettings()), getM(context.indexSettings()),
                    getEfConstruction(context.indexSettings()), copyTo.build(), this);
        }

        private String getSpaceType(Settings indexSettings) {
            String spaceType =  indexSettings.get(INDEX_KNN_SPACE_TYPE.getKey());
            if (spaceType == null) {
                logger.info("[KNN] The setting \"" + KNNConstants.SPACE_TYPE + "\" was not set for the index. " +
                        "Likely caused by recent version upgrade. Setting the setting to the default value="
                        + INDEX_KNN_DEFAULT_SPACE_TYPE);
                return INDEX_KNN_DEFAULT_SPACE_TYPE;
            }
            return spaceType;
        }

        private String getM(Settings indexSettings) {
            String m =  indexSettings.get(INDEX_KNN_ALGO_PARAM_M_SETTING.getKey());
            if (m == null) {
                logger.info("[KNN] The setting \"" + KNNConstants.HNSW_ALGO_M + "\" was not set for the index. " +
                        "Likely caused by recent version upgrade. Setting the setting to the default value="
                        + INDEX_KNN_DEFAULT_ALGO_PARAM_M);
                return String.valueOf(INDEX_KNN_DEFAULT_ALGO_PARAM_M);
            }
            return m;
        }

        private String getEfConstruction(Settings indexSettings) {
            String efConstruction =  indexSettings.get(INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING.getKey());
            if (efConstruction == null) {
                logger.info("[KNN] The setting \"" + KNNConstants.HNSW_ALGO_EF_CONSTRUCTION + "\" was not set for" +
                        " the index. Likely caused by recent version upgrade. Setting the setting to the default value="
                        + INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION);
                return String.valueOf(INDEX_KNN_DEFAULT_ALGO_PARAM_EF_CONSTRUCTION);
            }
            return efConstruction;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name);
            builder.parse(name, parserContext, node);

            if (builder.dimension.getValue() == -1) {
                throw new IllegalArgumentException("Dimension value missing for vector: " + name);
            }

            return builder;
        }
    }

    public static class KNNVectorFieldType extends MappedFieldType {

        int dimension;

        public KNNVectorFieldType(String name, Map<String, String> meta, int dimension) {
            super(name, false, true, TextSearchInfo.NONE, meta);
            this.dimension = dimension;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "KNN vector do not support exact searching, use KNN queries " +
                    "instead: [" + name() + "]");
        }
    }

    protected Explicit<Boolean> ignoreMalformed;
    private final boolean stored;
    private final boolean hasDocValues;
    protected final String spaceType;
    protected final String m;
    protected final String efConstruction;
    private final Integer dimension;

    public KNNVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields,
                                Explicit<Boolean> ignoreMalformed, String spaceType, String m, String efConstruction,
                                CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType,  multiFields, copyTo);
        this.stored = builder.stored.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.dimension = builder.dimension.getValue();
        this.ignoreMalformed = ignoreMalformed;
        this.spaceType = spaceType;
        this.m = m;
        this.efConstruction = efConstruction;
        this.fieldType = new FieldType(Defaults.FIELD_TYPE);
        this.fieldType.putAttribute(KNNConstants.SPACE_TYPE, spaceType);
        this.fieldType.putAttribute(KNNConstants.HNSW_ALGO_M, m);
        this.fieldType.putAttribute(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION, efConstruction);
        this.fieldType.freeze();
    }

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public KNNVectorFieldMapper clone() {
        return (KNNVectorFieldMapper) super.clone();
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
            FIELD_TYPE.putAttribute(KNN_FIELD, "true"); //This attribute helps to determine knn field type
            FIELD_TYPE.freeze();
        }
    }


    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (!KNNSettings.isKNNPluginEnabled()) {
            throw new IllegalStateException("KNN plugin is disabled. To enable " +
                    "update knn.plugin.enabled setting to true");
        }

        if (KNNSettings.isCircuitBreakerTriggered()) {
            throw new IllegalStateException("Indexing knn vector fields is rejected as circuit breaker triggered." +
                    " Check _opendistro/_knn/stats for detailed state");
        }

        context.path().add(simpleName());

        ArrayList<Float> vector = new ArrayList<>();
        XContentParser.Token token = context.parser().currentToken();
        float value;
        if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            while (token != XContentParser.Token.END_ARRAY) {
                value = context.parser().floatValue();

                if (Float.isNaN(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be NaN");
                }

                if (Float.isInfinite(value)) {
                    throw new IllegalArgumentException("KNN vector values cannot be infinity");
                }

                vector.add(value);
                token = context.parser().nextToken();
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = context.parser().floatValue();

            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be NaN");
            }

            if (Float.isInfinite(value)) {
                throw new IllegalArgumentException("KNN vector values cannot be infinity");
            }

            vector.add(value);
            context.parser().nextToken();
        }

        if (fieldType().dimension != vector.size()) {
            String errorMessage = String.format("Vector dimension mismatch. Expected: %d, Given: %d",
                    fieldType().dimension, vector.size());
            throw new IllegalArgumentException(errorMessage);
        }

        float[] array = new float[vector.size()];
        int i = 0;
        for (Float f : vector) {
            array[i++] = f;
        }

        VectorField point = new VectorField(name(), array, fieldType);

        context.doc().add(point);
        if (fieldType.stored()) {
            context.doc().add(new StoredField(name(), point.toString()));
        }
        context.path().remove();
    }

    @Override
    protected boolean docValuesByDefault() {
        return true;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new KNNVectorFieldMapper.Builder(simpleName()).init(this);
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public KNNVectorFieldType fieldType() {
        return (KNNVectorFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field(Names.IGNORE_MALFORMED, ignoreMalformed.value());
        }
        builder.field(KNNConstants.DIMENSION, fieldType().dimension);
    }
}
