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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class KNNESSettingsTestIT extends ESRestTestCase {

    public void createKNNIndex(String indexName) throws Exception {
        Settings settings = Settings.builder()
                                    .put("number_of_shards", 1)
                                    .put("number_of_replicas", 0)
                                    .put("index.knn", true)
                                    .build();
        String mapping = "\"properties\":{\"my_vector\":{\"type\":\"knn_vector\",\"dimension\":\"2\"}}";
        createIndex(indexName, settings, mapping);
    }

    public void indexKNNDoc(String indexName, float[] vector) throws Exception {
        Request indexRequest = new Request("PUT", "/" + indexName + "/_doc/1");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("my_vector", vector);
        builder.endObject();
        indexRequest.setJsonEntity(Strings.toString(builder));
        client().performRequest(indexRequest);
    }

    public Response makeGenericKnnQuery(String index, float[] vector, int k) throws Exception {
        Request request = new Request("POST", "/" + index + "/_search"
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                                                 .startObject("query")
                                                 .startObject("knn")
                                                 .startObject("my_vector")
                                                 .field("vector", vector)
                                                 .field("k", k)
                                                 .endObject()
                                                 .endObject()
                                                 .endObject()
                                                 .endObject();

        request.setJsonEntity(Strings.toString(builder));
        return client().performRequest(request);
    }

    public void updateSettings(String settingKey, Object value) throws Exception {
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

    public void getClusterSettings() throws Exception {
        Map<?, ?> getResponse = entityAsMap(adminClient().performRequest(new Request("GET", "/_cluster/settings")));
        Response response = client().performRequest(new Request("GET", "/_cluster/settings"));
        XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        XContentParser xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent());
        Map<String, Object> mp = xcp.map();
    }

    /**
     * KNN Index writes should be blocked when the plugin disabled
     * @throws Exception Exception from test
     */
    public void testIndexWritesPluginDisabled() throws Exception {
        String indexName = "testindex";
        createKNNIndex(indexName);

        float[] vector = {6.0f, 6.0f};
        indexKNNDoc(indexName, vector);

        float[] qvector = {1.0f, 2.0f};
        Response response = makeGenericKnnQuery(indexName, qvector, 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        //disable plugin
        updateSettings(KNNSettings.KNN_PLUGIN_ENABLED, false);

        // indexing should be blocked
        Exception ex = expectThrows(ResponseException.class,
                () -> indexKNNDoc(indexName, vector));
        assertThat(ex.getMessage(), containsString("KNN plugin is disabled"));

        //enable plugin
        updateSettings(KNNSettings.KNN_PLUGIN_ENABLED, true);
        indexKNNDoc(indexName, vector);
    }

    public void testQueriesPluginDisabled() throws Exception {
        String indexName = "testindex";
        createKNNIndex(indexName);

        float[] vector = {6.0f, 6.0f};
        indexKNNDoc(indexName, vector);

        float[] qvector = {1.0f, 2.0f};
        Response response = makeGenericKnnQuery(indexName, qvector, 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        //update settings
        updateSettings(KNNSettings.KNN_PLUGIN_ENABLED, false);

        // indexing should be blocked
        Exception ex = expectThrows(ResponseException.class,
                () -> makeGenericKnnQuery(indexName, qvector, 1));
        assertThat(ex.getMessage(), containsString("KNN plugin is disabled"));
        //enable plugin
        updateSettings(KNNSettings.KNN_PLUGIN_ENABLED, true);
        makeGenericKnnQuery(indexName, qvector, 1);
    }
}

