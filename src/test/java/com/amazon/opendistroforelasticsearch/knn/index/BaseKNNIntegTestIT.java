/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.plugin.KNNPlugin;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Basic integration test for KNN plugin
 *
 * Should have the following basic functionality:
 * 1. Create Index with KNN mapping  ## DONE
 * 2. Index Document  ## DONE
 * 3. Search Index  ## DONE
 * 4. Delete KNN index  ## DONE
 * 5. Delete KNN Document
 * 6. Call Stats API
 */
public class BaseKNNIntegTestIT extends ESRestTestCase {
    public static final String INDEX_NAME = "test_index";
    public static final String FIELD_NAME = "test_field";

    protected void createKnnIndex(String index, Settings settings) throws IOException {
        createIndex(index, settings);
    }

    protected void createKnnIndex(String index, String mapping) throws IOException {
        createIndex(index, getKNNDefaultSettings());
        makeIndexMappingRequest(index, mapping);
    }

    protected void createKnnIndex(String index, Settings settings, String mapping) throws IOException {
        createIndex(index, settings);
        makeIndexMappingRequest(index, mapping);
    }

    protected void addKnnDoc(String index, String docId, String fieldName, Object[] vector) throws IOException {
        makeIndexDocumentRequest(index, docId, fieldName, vector);
    }

    protected void updateKnnDoc(String index, String docId, String fieldName, Object[] vector) throws IOException {
        makeDocumentUpdateRequest(index, docId, fieldName, vector);
    }

    protected void deleteKnnDoc(String index, String docId) throws IOException {
        makeDocumentDeleteRequest(index, docId);
    }

    protected Response searchKNNIndex(String index, KNNQueryBuilder knnQueryBuilder, int resultSize) throws
            IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        knnQueryBuilder.doXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return makeIndexSearchRequest(index, builder, resultSize);
    }

    protected void deleteKNNIndex(String index) throws IOException {
        makeIndexDeleteRequest(index);
    }

    protected void forceMergeKnnIndex(String index) throws Exception {
        Request request = new Request(
                "POST",
                "/" + index + "/_refresh"
        );

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        request = new Request(
                "POST",
                "/" + index + "/_forcemerge"
        );

        request.addParameter("max_num_segments", "1");
        request.addParameter("flush", "true");
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        TimeUnit.SECONDS.sleep(5); // To make sure force merge is completed
    }

    protected void putMappingRequest(String index, String mapping) throws IOException {
        makeIndexMappingRequest(index, mapping);
    }

    protected String createKnnIndexMapping(String fieldName, Integer dimensions) throws IOException {
        return Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject(fieldName)
                .field("type", "knn_vector")
                .field("dimension", dimensions.toString())
                .endObject()
                .endObject()
                .endObject());
    }

    protected void updateSettings(String settingKey, Object value) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                       .startObject()
                       .startObject("persistent")
                       .field(settingKey, value)
                       .endObject()
                       .endObject();
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK,  RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected Response getKnnStats(List<String> nodeIds, List<String> stats) throws IOException {
        String nodePrefix = "";
        if (!nodeIds.isEmpty()) {
            nodePrefix = "/" + String.join(",", nodeIds);
        }

        String statsSuffix = "";
        if (!stats.isEmpty()) {
            statsSuffix = "/" + String.join(",", stats);
        }

        Request request = new Request(
                "GET",
                KNNPlugin.KNN_BASE_URI + nodePrefix + "/stats" + statsSuffix
        );

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK,  RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        return response;
    }

    private void makeDocumentDeleteRequest(String index, String docId) throws IOException {
        // Put KNN mapping
        Request request = new Request(
                "DELETE",
                "/" + index + "/_doc/" + docId + "?refresh"
        );

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void makeIndexMappingRequest(String index, String mapping) throws IOException {
        // Put KNN mapping
        Request request = new Request(
                "PUT",
                "/" + index + "/_mapping"
        );

        request.setJsonEntity(mapping);
        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void makeIndexDocumentRequest(String index, String docId, String fieldName, Object[] vector)
            throws IOException {
        Request request = new Request(
                "POST",
                "/" + index + "/_doc/" + docId + "?refresh=true"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field(fieldName, vector)
                .endObject();

        request.setJsonEntity(Strings.toString(builder));

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.CREATED,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void makeDocumentUpdateRequest(String index, String docId, String fieldName, Object[] vector)
            throws IOException {
        Request request = new Request(
                "POST",
                "/" + index + "/_doc/" + docId + "?refresh=true"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field(fieldName, vector)
                .endObject();

        request.setJsonEntity(Strings.toString(builder));

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private Response makeIndexSearchRequest(String index, XContentBuilder builder, Integer resultSize) throws
            IOException {
        Request request = new Request(
                "POST",
                "/" + index + "/_search"
        );

        request.addParameter("size", resultSize.toString());
        request.addParameter("explain", Boolean.toString(true));
        request.addParameter("search_type", "query_then_fetch");
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);

        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        return response;
    }

    private void makeIndexDeleteRequest(String index) throws IOException {
        Request request = new Request(
                "DELETE",
                "/" + index
        );

        Response response = client().performRequest(request);
        assertEquals(request.getEndpoint() + ": failed", RestStatus.OK,
                RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected Settings getKNNDefaultSettings() {
        return Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .put("index.knn", true)
                .build();
    }

    protected List<KNNResult> parseSearchResponse(String responseBody, String fieldName) throws IOException {
        @SuppressWarnings("unchecked")
        List<Object> hits = (List<Object>) ((Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody).map().get("hits")).get("hits");

        @SuppressWarnings("unchecked")
        List<KNNResult> knnSearchResponses = hits.stream().map(hit -> {
                    @SuppressWarnings("unchecked")
                    Float[] vector = Arrays.stream(
                            ((ArrayList<Float>) ((Map<String, Object>)
                                    ((Map<String, Object>) hit).get("_source")).get(fieldName)).toArray())
                            .map(Object::toString)
                            .map(Float::valueOf)
                            .toArray(Float[]::new);
                    return new KNNResult((String) ((Map<String, Object>) hit).get("_id"), vector);
                }
        ).collect(Collectors.toList());

        return knnSearchResponses;
    }

    protected Map<String, Object> parseClusterStatsResponse(String responseBody) throws IOException {
        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), responseBody).map();
        responseMap.remove("cluster_name");
        responseMap.remove("_nodes");
        responseMap.remove("nodes");
        return responseMap;
    }

    protected List<Map<String, Object>> parseNodeStatsResponse(String responseBody) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> responseMap = (Map<String, Object>)createParser(XContentType.JSON.xContent(),
                responseBody).map().get("nodes");

        // The key associated with the node that made the request
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodeResponses = responseMap.keySet().stream().map(key ->
            (Map<String, Object>) responseMap.get(key)
        ).collect(Collectors.toList());

        return nodeResponses;
    }
}