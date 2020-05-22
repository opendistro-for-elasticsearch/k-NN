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

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryShardContext;
import org.mockito.Mockito;

public class KNNQueryBuilderTests extends KNNTestCase {

    public void testInvalidK() {
        float[] queryVector = {1.0f, 1.0f};

        /**
         * -ve k
         */
        expectThrows(IllegalArgumentException.class,
                () ->  new KNNQueryBuilder("myvector", queryVector, -1));

        /**
         * zero k
         */
        expectThrows(IllegalArgumentException.class,
                () ->  new KNNQueryBuilder("myvector", queryVector, 0));

        /**
         * k > KNNQueryBuilder.K_MAX
         */
        expectThrows(IllegalArgumentException.class,
                () ->  new KNNQueryBuilder("myvector", queryVector, KNNQueryBuilder.K_MAX + 1));
    }

    public void testEmptyVector() {
        /**
         * null query vector
         */
        float[] queryVector = null;
        expectThrows(IllegalArgumentException.class,
                () -> new KNNQueryBuilder("myvector", queryVector, 1));

        /**
         * empty query vector
         */
        float[] queryVector1 = {};
        expectThrows(IllegalArgumentException.class,
                () -> new KNNQueryBuilder("myvector", queryVector1, 1));
    }

    public void testFromXcontent() throws Exception {
        float[] queryVector = {1.0f, 2.0f, 3.0f, 4.0f};
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("myvector", queryVector, 1);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(knnQueryBuilder.fieldName());
        builder.field(KNNQueryBuilder.VECTOR_FIELD.getPreferredName(), knnQueryBuilder.vector());
        builder.field(KNNQueryBuilder.K_FIELD.getPreferredName(), knnQueryBuilder.getK());
        builder.endObject();
        builder.endObject();
        XContentParser contentParser = createParser(builder);
        contentParser.nextToken();
        KNNQueryBuilder actualBuilder = KNNQueryBuilder.fromXContent(contentParser);
        actualBuilder.equals(knnQueryBuilder);
    }

    public void testDoToQuery() throws Exception {
        float[] queryVector = {1.0f, 2.0f, 3.0f, 4.0f};
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder("myvector", queryVector, 1);
        Index dummyIndex = new Index("dummy", "dummy");
        QueryShardContext mockQueryShardContext = Mockito.mock(QueryShardContext.class);
        Mockito.when(mockQueryShardContext.index()).thenReturn(dummyIndex);
        KNNQuery query = (KNNQuery)knnQueryBuilder.doToQuery(mockQueryShardContext);
        assertEquals(knnQueryBuilder.getK(), query.getK());
        assertEquals(knnQueryBuilder.fieldName(), query.getField());
        assertEquals(knnQueryBuilder.vector(), query.getQueryVector());
    }
}