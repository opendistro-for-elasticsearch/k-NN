/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Field Mapper for KNN vector type.
 */
public class KNNVectorFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "knn_vector";
    public static final String KNN_FIELD = "knn_field";

    static final int MAX_DIMENSION = 10000;

    protected Explicit<Boolean> ignoreMalformed;

    public KNNVectorFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Settings indexSettings, MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
    }

    public static class Names {
        public static final String IGNORE_MALFORMED = "ignore_malformed";
    }

    public static class Defaults {
        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final KNNVectorFieldType FIELD_TYPE = new KNNVectorFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
            FIELD_TYPE.putAttribute(KNN_FIELD, "true"); //This attribute helps to determine knn field type
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, KNNVectorFieldMapper> {
        protected Boolean ignoreMalformed;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        public Builder spaceTypeParam(String key, String paramValue) {
            Defaults.FIELD_TYPE.putAttribute(key, paramValue.toLowerCase());
            return builder;
        }

        public Builder algoParams(String key, int paramValue) {
            Defaults.FIELD_TYPE.putAttribute(key, String.valueOf(paramValue));
            return builder;
        }

        @Override
        public KNNVectorFieldType fieldType() {
            return (KNNVectorFieldType) super.fieldType();
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

        public KNNVectorFieldMapper build(BuilderContext context, String simpleName, MappedFieldType fieldType,
                                          MappedFieldType defaultFieldType, Settings indexSettings,
                                          MultiFields multiFields, Explicit<Boolean> ignoreMalformed,
                                          CopyTo copyTo) {
            setupFieldType(context);
            return new KNNVectorFieldMapper(simpleName, fieldType, defaultFieldType, indexSettings, multiFields,
                    ignoreMalformed, copyTo);
        }

        @Override
        public KNNVectorFieldMapper build(BuilderContext context) {
            return build(context, name, fieldType, defaultFieldType, context.indexSettings(),
                    multiFieldsBuilder.build(this, context), ignoreMalformed(context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
                throws MapperParsingException {
            Builder builder = new KNNVectorFieldMapper.Builder(name);
            builder.spaceTypeParam(KNNConstants.SPACE_TYPE, parserContext.mapperService().getIndexSettings().getValue(
                KNNSettings.INDEX_KNN_SPACE_TYPE));
            builder.algoParams(KNNConstants.HNSW_ALGO_M, parserContext.mapperService().getIndexSettings().getValue(
                    KNNSettings.INDEX_KNN_ALGO_PARAM_M_SETTING));
            builder.algoParams(KNNConstants.HNSW_ALGO_EF_CONSTRUCTION, parserContext.mapperService().getIndexSettings()
                    .getValue(KNNSettings.INDEX_KNN_ALGO_PARAM_EF_CONSTRUCTION_SETTING));

            /**
             * If dimension not provided. Throw Exception
             */
            Object dimensionNode = node.remove("dimension");
            if(dimensionNode == null) {
                throw new MapperParsingException("Dimension value missing for vector: " + name);
            }
            int dimensionValue = XContentMapValues.nodeIntegerValue(dimensionNode);
            /**
             * If dimension value greater than MAX_DIMENSION, throw exception
             */
            if(dimensionValue > MAX_DIMENSION) {
                throw new MapperParsingException("Dimension value cannot be greater than " +
                        MAX_DIMENSION + " for vector: " + name);
            }
            /**
             *  If trying to update an existing dimension, throw Exception
             */
            for (MappedFieldType fieldType : parserContext.mapperService().fieldTypes()) {
                if (name.equals(fieldType.name())) {
                    int originalDimension = ((KNNVectorFieldType)fieldType).dimension;
                    if (originalDimension != -1 && originalDimension != dimensionValue) {
                        String errorMessage = String.format("Dimension value cannot be updated. Previous value: %d, Current value: %d"
                                , originalDimension, dimensionValue);
                        throw new MapperParsingException(errorMessage);
                    }
                }
            }

            builder.fieldType().dimension = dimensionValue;
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();

                if (propName.equals(Names.IGNORE_MALFORMED)) {
                    builder.ignoreMalformed(XContentMapValues.nodeBooleanValue(propNode, name + "." + Names.IGNORE_MALFORMED));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    public static class KNNVectorFieldType extends MappedFieldType {

        int dimension = -1;

        public KNNVectorFieldType() {
        }

        KNNVectorFieldType(KNNVectorFieldType ref) {
            super(ref);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public MappedFieldType clone() {
            return new KNNVectorFieldType(this);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "KNN vector do not support exact searching, use KNN queries instead: ["
                                                           + name() + "]");
        }
    }

    @Override
    public final boolean parsesArrayValue() {
        return true;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
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

        VectorField point = new VectorField(name(), array, fieldType());

        context.doc().add(point);
        if (fieldType().stored()) {
            context.doc().add(new StoredField(name(), point.toString()));
        }
        context.path().remove();
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
        builder.field("dimension", fieldType().dimension);
    }
}
