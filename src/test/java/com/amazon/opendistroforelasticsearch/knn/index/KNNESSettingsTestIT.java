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

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.security.InvalidParameterException;

import static org.hamcrest.Matchers.containsString;

public class KNNESSettingsTestIT extends KNNRestTestCase {
    /**
     * KNN Index writes should be blocked when the plugin disabled
     * @throws Exception Exception from test
     */
    public void testIndexWritesPluginDisabled() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));

        Float[] vector = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        float[] qvector = {1.0f, 2.0f};
        Response response = searchKNNIndex(INDEX_NAME, new KNNQueryBuilder(FIELD_NAME, qvector, 1), 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        //disable plugin
        updateClusterSettings(KNNSettings.KNN_PLUGIN_ENABLED, false);

        // indexing should be blocked
        Exception ex = expectThrows(ResponseException.class,
                () -> addKnnDoc(INDEX_NAME, "2", FIELD_NAME, vector));
        assertThat(ex.getMessage(), containsString("KNN plugin is disabled"));

        //enable plugin
        updateClusterSettings(KNNSettings.KNN_PLUGIN_ENABLED, true);
        addKnnDoc(INDEX_NAME, "3", FIELD_NAME, vector);
    }

    public void testQueriesPluginDisabled() throws Exception {
        createKnnIndex(INDEX_NAME, createKnnIndexMapping(FIELD_NAME, 2));

        Float[] vector = {6.0f, 6.0f};
        addKnnDoc(INDEX_NAME, "1", FIELD_NAME, vector);

        float[] qvector = {1.0f, 2.0f};
        Response response = searchKNNIndex(INDEX_NAME, new KNNQueryBuilder(FIELD_NAME, qvector, 1), 1);
        assertEquals("knn query failed", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        //update settings
        updateClusterSettings(KNNSettings.KNN_PLUGIN_ENABLED, false);

        // indexing should be blocked
        Exception ex = expectThrows(ResponseException.class,
                () -> searchKNNIndex(INDEX_NAME, new KNNQueryBuilder(FIELD_NAME, qvector, 1), 1));
        assertThat(ex.getMessage(), containsString("KNN plugin is disabled"));
        //enable plugin
        updateClusterSettings(KNNSettings.KNN_PLUGIN_ENABLED, true);
        searchKNNIndex(INDEX_NAME, new KNNQueryBuilder(FIELD_NAME, qvector, 1), 1);
    }

    public void testCreateIndexWithInvalidSpaceType() throws IOException {
        String invalidSpaceType = "bar";
        Settings invalidSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .put("index.knn", true)
            .put("index.knn.space_type", invalidSpaceType)
            .build();
        Exception ex = expectThrows(ResponseException.class,
            () -> createKnnIndex(INDEX_NAME, invalidSettings, createKnnIndexMapping(FIELD_NAME, 2)));
        assertThat(ex.getMessage(), containsString(String.format("Unsupported space type: %s", invalidSpaceType)));
    }
}

