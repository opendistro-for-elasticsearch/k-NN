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
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

public class KNNScriptScoringIT extends KNNRestTestCase {

    public void testKNNL2ScriptScore() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] f1  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f1);

        Float[] f2  = {2.0f, 2.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, f2);

        Float[] f3  = {4.0f, 4.0f};
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, f3);

        Float[] f4  = {3.0f, 3.0f};
        addKnnDoc(INDEX_NAME, "4", FIELD_NAME, f4);


        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "vector": [2.0, 2.0]
         *      }
         */
        float[] queryVector = {1.0f, 1.0f};
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.L2);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "4", "3", "1");

        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(4, results.size());

        // assert document order
        assertEquals("2", results.get(0).getDocId());
        assertEquals("4", results.get(1).getDocId());
        assertEquals("3", results.get(2).getDocId());
        assertEquals("1", results.get(3).getDocId());
    }

    public void testKNNL1ScriptScore() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] f1  = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f1);

        Float[] f2  = {4.0f, 1.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, f2);

        Float[] f3  = {3.0f, 3.0f};
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, f3);

        Float[] f4  = {5.0f, 5.0f};
        addKnnDoc(INDEX_NAME, "4", FIELD_NAME, f4);


        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "vector": [1.0, 1.0]
         *      }
         */
        float[] queryVector = {1.0f, 1.0f};
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.L1);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("2", "4", "3", "1");

        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(4, results.size());

        // assert document order
        assertEquals("2", results.get(0).getDocId());
        assertEquals("3", results.get(1).getDocId());
        assertEquals("4", results.get(2).getDocId());
        assertEquals("1", results.get(3).getDocId());
    }

    public void testKNNCosineScriptScore() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] f1  = {1.0f, -1.0f};
        addKnnDoc(INDEX_NAME, "0", FIELD_NAME, f1);

        Float[] f2  = {1.0f, 0.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f2);

        Float[] f3  = {1.0f, 1.0f};
        addKnnDoc(INDEX_NAME, "2", FIELD_NAME, f3);

        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "query_value": [2.0, 2.0],
         *       "space_type": "L2"
         *      }
         *
         *
         */
        float[] queryVector = {2.0f, -2.0f};
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.COSINESIMIL);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<KNNResult> results = parseSearchResponse(EntityUtils.toString(response.getEntity()), FIELD_NAME);
        List<String> expectedDocids = Arrays.asList("0", "1", "2");

        List<String> actualDocids = new ArrayList<>();
        for(KNNResult result : results) {
            actualDocids.add(result.getDocId());
        }

        assertEquals(3, results.size());

        // assert document order
        assertEquals("0", results.get(0).getDocId());
        assertEquals("1", results.get(1).getDocId());
        assertEquals("2", results.get(2).getDocId());
    }

    public void testKNNInvalidSourceScript() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));

        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "query_value": [2.0, 2.0],
         *       "space_type": "cosinesimil"
         *      }
         */
        float[] queryVector = {2.0f, -2.0f};
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.COSINESIMIL);
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, KNNScoringScriptEngine.NAME, "Dummy_source", params);
        ScriptScoreQueryBuilder sc = new ScriptScoreQueryBuilder(qb, script);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("query");

        builder.startObject("script_score");
        builder.field("query");
        sc.query().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field("script", script);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        Request request = new Request(
                "POST",
                "/" + INDEX_NAME + "/_search"
        );

        request.setJsonEntity(Strings.toString(builder));
        ResponseException ex = expectThrows(ResponseException.class,  () -> client().performRequest(request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Unknown script name Dummy_source"));
    }

    public void testInvalidSpace() throws Exception {
        String INVALID_SPACE = "dummy";
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));

        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        float[] queryVector = {2.0f, -2.0f};
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", INVALID_SPACE);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        ResponseException ex = expectThrows(ResponseException.class,  () -> client().performRequest(request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Invalid space type. Please refer to the available space types"));
    }

    public void testMissingParamsInScript() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));

        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        float[] queryVector = {2.0f, -2.0f};
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.COSINESIMIL);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        ResponseException ex = expectThrows(ResponseException.class,  () -> client().performRequest(request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Missing parameter [field]"));

        // Remove query vector parameter
        params.put("field", FIELD_NAME);
        params.remove("query_value");
        Request vector_request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        ex = expectThrows(ResponseException.class,  () -> client().performRequest(vector_request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Missing parameter [query_value]"));

        // Remove space parameter
        params.put("query_value", queryVector);
        params.remove("space_type");
        Request space_request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        ex = expectThrows(ResponseException.class,  () -> client().performRequest(space_request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("Missing parameter [space_type]"));
    }

    public void testUnequalDimensions() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] f1  = {1.0f, -1.0f};
        addKnnDoc(INDEX_NAME, "0", FIELD_NAME, f1);

        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        float[] queryVector = {2.0f, -2.0f, -2.0f};  // query dimension and field dimension mismatch
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.COSINESIMIL);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        ResponseException ex = expectThrows(ResponseException.class,  () -> client().performRequest(request));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("does not match"));
    }

    @SuppressWarnings("unchecked")
    public void testKNNScoreforNonVectorDocument() throws Exception {
        /*
         * Create knn index and populate data
         */
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        Float[] f1  = {1.0f, 1.0f};
        addDocWithNumericField(INDEX_NAME, "0", "price", 10);
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, f1);
        forceMergeKnnIndex(INDEX_NAME);
        /**
         * Construct Search Request
         */
        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        float[] queryVector = {2.0f, 2.0f};  // query dimension and field dimension mismatch
        params.put("field", FIELD_NAME);
        params.put("query_value", queryVector);
        params.put("space_type", KNNConstants.L2);
        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        List<Object> hits = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody).map().get("hits")).get("hits");

        List<String>  docIds = hits.stream().map(hit -> {
            String id = ((String)((Map<String, Object>)hit).get("_id"));
            return id;
        }).collect(Collectors.toList());
        //assert document order
        assertEquals("1", docIds.get(0));
        assertEquals("0", docIds.get(1));

        List<Double>  scores = hits.stream().map(hit -> {
            Double score = ((Double)((Map<String, Object>)hit).get("_score"));
            return score;
        }).collect(Collectors.toList());
        //assert scores
        assertEquals(0.33333, scores.get(0), 0.001);
        assertEquals(Float.MIN_VALUE, scores.get(1), 0.001);
    }

    @SuppressWarnings("unchecked")
    public void testHammingScriptScore_Long() throws Exception {
        createIndex(INDEX_NAME, Settings.EMPTY);
        String longMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject(FIELD_NAME)
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject());
        putMappingRequest(INDEX_NAME, longMapping);

        addDocWithNumericField(INDEX_NAME, "0", FIELD_NAME, 8L);
        addDocWithNumericField(INDEX_NAME, "1", FIELD_NAME, 1L);
        addDocWithNumericField(INDEX_NAME, "2", FIELD_NAME, -9_223_372_036_818_523_493L);
        addDocWithNumericField(INDEX_NAME, "3", FIELD_NAME, 1_000_000_000_000_000L);

        // Add docs without the field. These docs should not appear in top 4 of results
        addDocWithNumericField(INDEX_NAME, "4", "price", 10);
        addDocWithNumericField(INDEX_NAME, "5", "price", 10);
        addDocWithNumericField(INDEX_NAME, "6", "price", 10);

        /*
         * Decimal to Binary conversions lookup
         * 8                          -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1000
         * 1                          -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001
         * -9_223_372_036_818_523_493 -> 1000 0000 0000 0000 0000 0000 0000 0000 0000 0010 0010 1001 0010 1010 1001 1011
         * 1_000_000_000_000_000      -> 0000 0000 0000 0011 1000 1101 0111 1110 1010 0100 1100 0110 1000 0000 0000 0000
         * -9_223_372_036_818_526_181 -> 1000 0000 0000 0000 0000 0000 0000 0000 0000 0010 0010 1001 0010 0000 0001 1011
         * 10                         -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1010
         */

        QueryBuilder qb1 = new MatchAllQueryBuilder();
        Map<String, Object> params1 = new HashMap<>();
        Long queryValue1 = -9223372036818526181L;
        params1.put("field", FIELD_NAME);
        params1.put("query_value", queryValue1);
        params1.put("space_type", KNNConstants.HAMMING_BIT);
        Request request1 = constructKNNScriptQueryRequest(INDEX_NAME, qb1, params1, 4);
        Response response1 = client().performRequest(request1);
        assertEquals(request1.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response1.getStatusLine().getStatusCode()));

        String responseBody1 = EntityUtils.toString(response1.getEntity());
        List<Object> hits1 = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody1).map().get("hits")).get("hits");

        List<String>  docIds1 = hits1.stream().map(hit ->
                ((String)((Map<String, Object>)hit).get("_id"))).collect(Collectors.toList());

        List<Double>  docScores1 = hits1.stream().map(hit ->
                ((Double)((Map<String, Object>)hit).get("_score"))).collect(Collectors.toList());

        double[] scores1 = new double[docScores1.size()];
        for (int i = 0; i < docScores1.size(); i++) {
            scores1[i] = docScores1.get(i);
        }

        List<String> correctIds1 = Arrays.asList("2", "0", "1", "3");
        double[] correctScores1 = new double[] {1.0/(1 + 3), 1.0/(1 + 9), 1.0/(1 + 9), 1.0/(1 + 30)};

        assertEquals(4, correctIds1.size());
        assertArrayEquals(correctIds1.toArray(), docIds1.toArray());
        assertArrayEquals(correctScores1, scores1, 0.001);

        /*
         * Force merge to one segment to confirm that docs without field are not included in the results when segment
         * is mixed with docs that have the field and docs that dont.
         */
        forceMergeKnnIndex(INDEX_NAME);

        QueryBuilder qb2 = new MatchAllQueryBuilder();
        Map<String, Object> params2 = new HashMap<>();
        Long queryValue2 = 10L;
        params2.put("field", FIELD_NAME);
        params2.put("query_value", queryValue2);
        params2.put("space_type", KNNConstants.HAMMING_BIT);
        Request request2 = constructKNNScriptQueryRequest(INDEX_NAME, qb2, params2, 4);
        Response response2 = client().performRequest(request2);
        assertEquals(request2.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response2.getStatusLine().getStatusCode()));

        String responseBody2 = EntityUtils.toString(response2.getEntity());
        List<Object> hits2 = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody2).map().get("hits")).get("hits");

        List<String>  docIds2 = hits2.stream().map(hit ->
                ((String)((Map<String, Object>)hit).get("_id"))).collect(Collectors.toList());

        List<Double>  docScores2 = hits2.stream().map(hit ->
                ((Double)((Map<String, Object>)hit).get("_score"))).collect(Collectors.toList());

        double[] scores2 = new double[docScores2.size()];
        for (int i = 0; i < docScores2.size(); i++) {
            scores2[i] = docScores2.get(i);
        }

        List<String> correctIds2 = Arrays.asList("0", "1", "2", "3");
        double[] correctScores2 = new double[] {1.0/(1 + 1), 1.0/(1 + 3), 1.0/(1 + 11), 1.0/(1 + 22)};

        assertEquals(4, correctIds2.size());
        assertArrayEquals(correctIds2.toArray(), docIds2.toArray());
        assertArrayEquals(correctScores2, scores2, 0.001);
    }

    @SuppressWarnings("unchecked")
    public void testHammingScriptScore_Base64() throws Exception  {
        createIndex(INDEX_NAME, Settings.EMPTY);
        String longMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject(FIELD_NAME)
                .field("type", "binary")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject());
        putMappingRequest(INDEX_NAME, longMapping);

        addDocWithBinaryField(INDEX_NAME, "0", FIELD_NAME, "AAAAAAAAAAk=");
        addDocWithBinaryField(INDEX_NAME, "1", FIELD_NAME, "AAAAAAAAAAE=");
        addDocWithBinaryField(INDEX_NAME, "2", FIELD_NAME, "gAAAAAIpKps=");
        addDocWithBinaryField(INDEX_NAME, "3", FIELD_NAME, "AAONfqTGgAA=");

        // Add docs without the field. These docs should not appear in top 4 of results
        addDocWithNumericField(INDEX_NAME, "4", "price", 10);
        addDocWithNumericField(INDEX_NAME, "5", "price", 10);
        addDocWithNumericField(INDEX_NAME, "6", "price", 10);

        /*
         * Base64 encodings to Binary conversions lookup
         * AAAAAAAAAAk=  -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1001
         * AAAAAAAAAAE=  -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001
         * gAAAAAIpKps=  -> 1000 0000 0000 0000 0000 0000 0000 0000 0000 0010 0010 1001 0010 1010 1001 1011
         * AAONfqTGgAA=  -> 0000 0000 0000 0011 1000 1101 0111 1110 1010 0100 1100 0110 1000 0000 0000 0000
         * gAAAAAIpIBs=  -> 1000 0000 0000 0000 0000 0000 0000 0000 0000 0010 0010 1001 0010 0000 0001 1011
         * AAAAAAIpIBs=  -> 0000 0000 0000 0000 0000 0000 0000 0000 0000 0010 0010 1001 0010 0000 0001 1011
         */

        QueryBuilder qb1 = new MatchAllQueryBuilder();
        Map<String, Object> params1 = new HashMap<>();
        String queryValue1 = "gAAAAAIpIBs=";
        params1.put("field", FIELD_NAME);
        params1.put("query_value", queryValue1);
        params1.put("space_type", KNNConstants.HAMMING_BIT);
        Request request1 = constructKNNScriptQueryRequest(INDEX_NAME, qb1, params1, 4);
        Response response1 = client().performRequest(request1);
        assertEquals(request1.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response1.getStatusLine().getStatusCode()));

        String responseBody1 = EntityUtils.toString(response1.getEntity());
        List<Object> hits1 = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody1).map().get("hits")).get("hits");

        List<String>  docIds1 = hits1.stream().map(hit ->
                ((String)((Map<String, Object>)hit).get("_id"))).collect(Collectors.toList());

        List<Double>  docScores1 = hits1.stream().map(hit ->
                ((Double)((Map<String, Object>)hit).get("_score"))).collect(Collectors.toList());

        double[] scores1 = new double[docScores1.size()];
        for (int i = 0; i < docScores1.size(); i++) {
            scores1[i] = docScores1.get(i);
        }

        List<String> correctIds1 = Arrays.asList("2", "0", "1", "3");
        double[] correctScores1 = new double[] {1.0/(1 + 3), 1.0/(1 + 8), 1.0/(1 + 9), 1.0/(1 + 30)};

        assertEquals(correctIds1.size(), docIds1.size());
        assertArrayEquals(correctIds1.toArray(), docIds1.toArray());
        assertArrayEquals(correctScores1, scores1, 0.001);

        /*
         * Force merge to one segment to confirm that docs without field are not included in the results when segment
         * is mixed with docs that have the field and docs that dont.
         */
        forceMergeKnnIndex(INDEX_NAME);

        QueryBuilder qb2 = new MatchAllQueryBuilder();
        Map<String, Object> params2 = new HashMap<>();
        String queryValue2 = "AAAAAAIpIBs=";
        params2.put("field", FIELD_NAME);
        params2.put("query_value", queryValue2);
        params2.put("space_type", KNNConstants.HAMMING_BIT);
        Request request2 = constructKNNScriptQueryRequest(INDEX_NAME, qb2, params2, 4);
        Response response2 = client().performRequest(request2);
        assertEquals(request2.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response2.getStatusLine().getStatusCode()));

        String responseBody2 = EntityUtils.toString(response2.getEntity());
        List<Object> hits2 = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody2).map().get("hits")).get("hits");

        List<String>  docIds2 = hits2.stream().map(hit ->
                ((String)((Map<String, Object>)hit).get("_id"))).collect(Collectors.toList());

        List<Double>  docScores2 = hits2.stream().map(hit ->
                ((Double)((Map<String, Object>)hit).get("_score"))).collect(Collectors.toList());

        double[] scores2 = new double[docScores2.size()];
        for (int i = 0; i < docScores2.size(); i++) {
            scores2[i] = docScores2.get(i);
        }

        List<String> correctIds2 = Arrays.asList("2", "0", "1", "3");
        double[] correctScores2 = new double[] {1.0/(1 + 4), 1.0/(1 + 7), 1.0/(1 + 8), 1.0/(1 + 29)};

        assertEquals(correctIds2.size(), docIds2.size());
        assertArrayEquals(correctIds2.toArray(), docIds2.toArray());
        assertArrayEquals(correctScores2, scores2, 0.001);
    }
}
