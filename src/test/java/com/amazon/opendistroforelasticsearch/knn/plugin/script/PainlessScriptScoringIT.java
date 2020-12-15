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
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PainlessScriptScoringIT extends KNNRestTestCase {


    private Map<String, Float[]> getL2TestData() {
        Map<String, Float[]> data = new HashMap<>();
        data.put("1", new Float[]{6.0f, 6.0f});
        data.put("2", new Float[]{2.0f, 2.0f});
        data.put("3", new Float[]{4.0f, 4.0f});
        data.put("4", new Float[]{3.0f, 3.0f});
        return data;
    }

    private Map<String, Float[]> getCosineTestData() {
        Map<String, Float[]> data = new HashMap<>();
        data.put("0", new Float[]{1.0f, -1.0f});
        data.put("2", new Float[]{1.0f, 1.0f});
        data.put("1", new Float[]{1.0f, 0.0f});
        return data;
    }

    private void buildTestIndex(Map<String, Float[]> documents) throws IOException {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));
        for (Map.Entry<String, Float[]> data : documents.entrySet()) {
            addKnnDoc(INDEX_NAME, data.getKey(), FIELD_NAME, data.getValue());
        }
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

    public void testCosineSimilarityScriptScore() throws Exception {
        float[] queryVector = {2.0f, -2.0f};
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

    public void testCosineSimilarityNormalizedScriptScore() throws Exception {
        float[] queryVector = {2.0f, -2.0f};
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
}
