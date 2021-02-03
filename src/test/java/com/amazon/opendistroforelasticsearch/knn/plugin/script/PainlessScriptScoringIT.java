/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;
import com.amazon.opendistroforelasticsearch.knn.KNNResult;
import com.amazon.opendistroforelasticsearch.knn.index.KNNVectorFieldMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PainlessScriptScoringIT extends KNNRestTestCase {

    private static String NUMERIC_INDEX_FIELD_NAME = "price";

    /**
     * Utility to create a Index Mapping with multiple fields
     */
    protected String createMapping(List<MappingProperty> properties) throws IOException {
        Objects.requireNonNull(properties);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (MappingProperty property : properties) {
            XContentBuilder builder = xContentBuilder.startObject(property.getName())
                    .field("type", property.getType());
            if (property.getDimension() != null) {
                builder.field("dimension", property.getDimension());
            }
            builder.endObject();
        }
        xContentBuilder.endObject().endObject();
        return Strings.toString(xContentBuilder);
    }

    /*
     creates KnnIndex based on properties, we add single non-knn vector documents to verify whether actions
     works on non-knn vector documents as well
     */
    private void buildTestIndex(Map<String, Float[]> knnDocuments) throws Exception {
        List<MappingProperty> properties = buildMappingProperties();
        createKnnIndex(INDEX_NAME, createMapping(properties));
        for (Map.Entry<String, Float[]> data : knnDocuments.entrySet()) {
            addKnnDoc(INDEX_NAME, data.getKey(), FIELD_NAME, data.getValue());
        }
    }

    private Map<String, Float[]> getL2TestData() {
        Map<String, Float[]> data = new HashMap<>();
        data.put("1", new Float[]{6.0f, 6.0f});
        data.put("2", new Float[]{2.0f, 2.0f});
        data.put("3", new Float[]{4.0f, 4.0f});
        data.put("4", new Float[]{3.0f, 3.0f});
        return data;
    }
    private Map<String, Float[]> getL1TestData() {
        Map<String, Float[]> data = new HashMap<>();
        data.put("1", new Float[]{6.0f, 6.0f});
        data.put("2", new Float[]{4.0f, 1.0f});
        data.put("3", new Float[]{3.0f, 3.0f});
        data.put("4", new Float[]{5.0f, 5.0f});
        return data;
    }

    private Map<String, Float[]> getCosineTestData() {
        Map<String, Float[]> data = new HashMap<>();
        data.put("0", new Float[]{1.0f, -1.0f});
        data.put("2", new Float[]{1.0f, 1.0f});
        data.put("1", new Float[]{1.0f, 0.0f});
        return data;
    }

    /*
     The doc['field'] will throw an error if field is missing from the mappings.
     */
    private List<MappingProperty> buildMappingProperties() {
        List<MappingProperty> properties = new ArrayList<>();
        properties.add(new MappingProperty(FIELD_NAME, KNNVectorFieldMapper.CONTENT_TYPE).dimension("2"));
        properties.add(new MappingProperty(NUMERIC_INDEX_FIELD_NAME, "integer"));
        return properties;
    }

    public void testL2ScriptScoreFails() throws Exception {
        String source = String.format("1/(1 + l2Squared([1.0f, 1.0f], doc['%s']))", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL2TestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        expectThrows(ResponseException.class, () -> client().performRequest(request));
        deleteKNNIndex(INDEX_NAME);
    }

    private Request buildPainlessScriptRequest(
            String source, int size, Map<String, Float[]> documents) throws Exception {
        buildTestIndex(documents);
        QueryBuilder qb = new MatchAllQueryBuilder();
        return constructScriptQueryRequest(
                INDEX_NAME, qb, Collections.emptyMap(), Script.DEFAULT_SCRIPT_LANG, source, size);
    }

    public void testL2ScriptScore() throws Exception {

        String source = String.format("1/(1 + l2Squared([1.0f, 1.0f], doc['%s']))", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL2TestData());

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());


        String[] expectedDocIDs = {"2", "4", "3", "1"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    public void testL2ScriptScoreWithNumericField() throws Exception {

        String source = String.format(
                "doc['%s'].size() == 0 ? 0 : 1/(1 + l2Squared([1.0f, 1.0f], doc['%s']))", FIELD_NAME, FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL2TestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());


        String[] expectedDocIDs = {"2", "4", "3", "1"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    public void testCosineSimilarityScriptScoreFails() throws Exception {
        String source = String.format("1 + cosineSimilarity([2.0f, -2.0f], doc['%s'])", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        expectThrows(ResponseException.class, () -> client().performRequest(request));
        deleteKNNIndex(INDEX_NAME);
    }

    public void testCosineSimilarityScriptScore() throws Exception {
        String source = String.format("1 + cosineSimilarity([2.0f, -2.0f], doc['%s'])", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());

        String[] expectedDocIDs = {"0", "1", "2"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    public void testCosineSimilarityScriptScoreWithNumericField() throws Exception {
        String source = String.format(
                "doc['%s'].size() == 0 ? 0 : 1 + cosineSimilarity([2.0f, -2.0f], doc['%s'])", FIELD_NAME, FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());

        String[] expectedDocIDs = {"0", "1", "2"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    // test fails without size check before executing method
    public void testCosineSimilarityNormalizedScriptScoreFails() throws Exception {
        String source = String.format("1 + cosineSimilarity([2.0f, -2.0f], doc['%s'], 3.0f)", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        expectThrows(ResponseException.class, () -> client().performRequest(request));
        deleteKNNIndex(INDEX_NAME);
    }

    public void testCosineSimilarityNormalizedScriptScore() throws Exception {
        String source = String.format("1 + cosineSimilarity([2.0f, -2.0f], doc['%s'], 3.0f)", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());

        String[] expectedDocIDs = {"0", "1", "2"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    public void testCosineSimilarityNormalizedScriptScoreWithNumericField() throws Exception {
        String source = String.format(
                "doc['%s'].size() == 0 ? 0 : 1 + cosineSimilarity([2.0f, -2.0f], doc['%s'], 3.0f)",
                FIELD_NAME, FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getCosineTestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());

        String[] expectedDocIDs = {"0", "1", "2"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    // L1 tests
    public void testL1ScriptScoreFails() throws Exception {
        String source = String.format("1/(1 + l1Norm([1.0f, 1.0f], doc['%s']))", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL1TestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        expectThrows(ResponseException.class, () -> client().performRequest(request));
        deleteKNNIndex(INDEX_NAME);
    }
    public void testL1ScriptScore() throws Exception {

        String source = String.format("1/(1 + l1Norm([1.0f, 1.0f], doc['%s']))", FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL1TestData());

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());


        String[] expectedDocIDs = {"2", "3", "4", "1"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }

    public void testL1ScriptScoreWithNumericField() throws Exception {

        String source = String.format(
                "doc['%s'].size() == 0 ? 0 : 1/(1 + l1Norm([1.0f, 1.0f], doc['%s']))", FIELD_NAME, FIELD_NAME);
        Request request = buildPainlessScriptRequest(source, 3, getL1TestData());
        addDocWithNumericField(INDEX_NAME, "100", NUMERIC_INDEX_FIELD_NAME, 1000);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        assertEquals(3, results.size());


        String[] expectedDocIDs = {"2", "3", "4", "1"};
        for (int i = 0; i < results.size(); i++) {
            assertEquals(expectedDocIDs[i], results.get(i).getDocId());
        }
        deleteKNNIndex(INDEX_NAME);
    }


    class MappingProperty {

        private String name;
        private String type;
        private String dimension;

        MappingProperty(String name, String type) {
            this.name = name;
            this.type = type;
        }

        MappingProperty dimension(String dimension) {
            this.dimension = dimension;
            return this;
        }

        String getDimension() {
            return dimension;
        }

        String getName() {
            return name;
        }

        String getType() {
            return type;
        }
    }
}

