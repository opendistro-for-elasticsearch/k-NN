package com.amazon.opendistroforelasticsearch.knn.plugin.scripts;

import com.amazon.opendistroforelasticsearch.knn.KNNRestTestCase;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringScriptEngine;
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

import java.util.HashMap;
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
        int k =1;
        float[] queryVector = {1.0f, 1.0f};
        params.put("field", "my_dense_vector");
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

        request.addParameter("size", Integer.toString(k));
        request.setJsonEntity(Strings.toString(builder));

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

}
