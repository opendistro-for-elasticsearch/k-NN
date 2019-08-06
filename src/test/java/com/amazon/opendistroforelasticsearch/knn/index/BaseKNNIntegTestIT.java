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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;

public class BaseKNNIntegTestIT extends ESIntegTestCase {
    protected void addKnnDoc(String index, String docId, Object[] vector) throws IOException {
        IndexResponse response = client().prepareIndex(index, "_doc", docId)
                                         .setSource(XContentFactory.jsonBuilder()
                                                                   .startObject()
                                                                   .array("my_vector", vector)
                                                                   .field("price", 10)
                                                                   .endObject())
                                         .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                         .get();
        if(!response.status().equals(RestStatus.OK) && !response.status().equals(RestStatus.CREATED)) {
            fail("Bad response while adding doc");
        }
    }

    protected void addKnnDocWithField(String index, String docId, Object[] vector, String fieldname) throws IOException {
        IndexResponse response = client().prepareIndex(index, "_doc", docId)
                                         .setSource(XContentFactory.jsonBuilder()
                                                                   .startObject()
                                                                   .array(fieldname, vector)
                                                                   .field("price", 10)
                                                                   .endObject())
                                         .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                         .get();
        if(!response.status().equals(RestStatus.OK) && !response.status().equals(RestStatus.CREATED)) {
            fail("Bad response while adding doc");
        }
    }

    protected SearchResponse searchKNNIndex(String index, int resultSize, KNNQueryBuilder knnQueryBuilder) {
        logger.info("Searching KNN index " + index );
        SearchResponse searchResponse = client().prepareSearch(index)
                                                .setSearchType(SearchType.QUERY_THEN_FETCH)
                                                .setQuery(knnQueryBuilder)  // Query
                                                .setSize(resultSize)
                                                .setExplain(true)
                                                .get();
        assertEquals(searchResponse.status(), RestStatus.OK);
        return searchResponse;
    }

    protected void createKnnIndex(String index, Settings settings) {
        createIndex(index, settings==null ? indexSettings() : settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");
        request.source("my_vector", "type=knn_vector,dimension=2");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());
    }

    protected Settings createIndexDefaultSettings() {
        Settings settings = Settings.builder()
                                    .put(super.indexSettings())
                                    .put("index.knn", true)
                                    .build();
        return settings;
    }

    protected void createIndexAndFieldWithDefaultSettings() {
        Settings settings = createIndexDefaultSettings();
        String index = "testindex";
        createIndex(index, settings);
        PutMappingRequest request = new PutMappingRequest(index).type("_doc");
        request.source("my_vector", "type=knn_vector,dimension=4");
        ElasticsearchAssertions.assertAcked(client().admin().indices().putMapping(request).actionGet());
    }
}

