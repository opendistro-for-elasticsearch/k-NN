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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

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

    private void makeDocumentDeleteRequest(String index, String docId) throws IOException {
        // Put KNN mapping
        Request request = new Request(
                "DELETE",
                "/" + index + "/_doc/" + docId
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
}