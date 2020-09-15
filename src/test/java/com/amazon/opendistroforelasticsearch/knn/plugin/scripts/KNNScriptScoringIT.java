package com.amazon.opendistroforelasticsearch.knn.plugin.scripts;

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;
import com.amazon.opendistroforelasticsearch.knn.KNNResult;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringScriptEngine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        Request request = new Request(
                "POST",
                "/" + INDEX_NAME + "/_search"
        );

        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "vector": [2.0, 2.0]
         *      }
         *
         *
         */
        float[] queryVector = {1.0f, 1.0f};
        params.put("field", FIELD_NAME);
        params.put("vector", queryVector);
        params.put("space", KNNConstants.L2);
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, KNNScoringScriptEngine.NAME, KNNScoringScriptEngine.SCRIPT_SOURCE, params);
        ScriptScoreQueryBuilder sc = new ScriptScoreQueryBuilder(qb, script);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("query");

        builder.startObject("script_score");
        builder.field("query");
        sc.query().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field("script", script);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        request = new Request(
                "POST",
                "/" + INDEX_NAME + "/_search"
        );

        request.setJsonEntity(Strings.toString(builder));

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
        Request request = new Request(
                "POST",
                "/" + INDEX_NAME + "/_search"
        );

        QueryBuilder qb = new MatchAllQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        /*
         *   params": {
         *       "field": "my_dense_vector",
         *       "vector": [2.0, 2.0]
         *      }
         *
         *
         */
        float[] queryVector = {2.0f, -2.0f};
        params.put("field", FIELD_NAME);
        params.put("vector", queryVector);
        params.put("space", KNNConstants.COSINESIMIL);
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, KNNScoringScriptEngine.NAME, KNNScoringScriptEngine.SCRIPT_SOURCE, params);
        ScriptScoreQueryBuilder sc = new ScriptScoreQueryBuilder(qb, script);

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("query");

        builder.startObject("script_score");
        builder.field("query");
        sc.query().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field("script", script);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        request = new Request(
                "POST",
                "/" + INDEX_NAME + "/_search"
        );

        request.setJsonEntity(Strings.toString(builder));

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



}
