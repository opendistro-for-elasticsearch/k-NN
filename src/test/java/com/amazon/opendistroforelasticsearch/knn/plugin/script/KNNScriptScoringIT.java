package com.amazon.opendistroforelasticsearch.knn.plugin.script;

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;
import com.amazon.opendistroforelasticsearch.knn.KNNResult;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
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
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()),
                containsString("query vector and field vector dimensions mismatch"));
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

//    @SuppressWarnings("unchecked")
    public void testHammingScriptScore_Long() throws Exception {
//        // Create index with long values in it
//        createIndex(INDEX_NAME, Settings.EMPTY);
//        String longMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
//                .startObject("properties")
//                .startObject(FIELD_NAME)
//                .field("type", "long")
//                .endObject()
//                .endObject()
//                .endObject());
//        putMappingRequest(INDEX_NAME, longMapping);
//
//        // Add a couple docs to the index
//        addDocWithNumericField(INDEX_NAME, "0", FIELD_NAME, 0L);
//        addDocWithNumericField(INDEX_NAME, "1", FIELD_NAME, 1L);
//        addDocWithNumericField(INDEX_NAME, "2", FIELD_NAME, 2L);
//
//        // Construct search request
//        QueryBuilder qb = new MatchAllQueryBuilder();
//        Map<String, Object> params = new HashMap<>();
//        long queryLong = 0L;  // query dimension and field dimension mismatch
//        params.put("field", FIELD_NAME);
//        params.put("long_encoding", queryLong);
//        params.put("space_type", KNNConstants.BIT_HAMMING);
//        Request request = constructKNNScriptQueryRequest(INDEX_NAME, qb, params);
//        Response response = client().performRequest(request);
//        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
//                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
//
//        String responseBody = EntityUtils.toString(response.getEntity());
//        List<Object> hits = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
//                responseBody).map().get("hits")).get("hits");
//
//        List<String>  docIds = hits.stream().map(hit -> {
//            String id = ((String)((Map<String, Object>)hit).get("_id"));
//            return id;
//        }).collect(Collectors.toList());
//
//        List<Double>  docScores = hits.stream().map(hit -> {
//            Double score = ((Double)((Map<String, Object>)hit).get("_score"));
//            return score;
//        }).collect(Collectors.toList());
//
//        logger.info("Ids = " + docIds);
//        logger.info("Scores = " + docScores);
//
//        fail();

    }

    public void testHammingScriptScore_Base64() {

    }
}
