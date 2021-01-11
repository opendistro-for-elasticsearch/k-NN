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
package com.amazon.opendistroforelasticsearch.knn.index;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.junit.Before;

import java.io.IOException;

public class KNNVectorIndexFieldDataTests extends KNNTestCase {

    private static final String MOCK_INDEX_FIELD_NAME = "test-index-field-name";
    private KNNVectorIndexFieldData indexFieldData;
    private Directory directory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexFieldData = new KNNVectorIndexFieldData(MOCK_INDEX_FIELD_NAME, CoreValuesSourceType.BYTES);
        directory = newDirectory();
        createEmptyDocument(directory);
    }

    private void createEmptyDocument(Directory directory) throws IOException {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, conf);
        writer.addDocument(new Document());
        writer.commit();
        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }

    public void testGetFieldName() {
        assertEquals(MOCK_INDEX_FIELD_NAME, indexFieldData.getFieldName());
    }

    public void testGetValuesSourceType() {
        assertEquals(CoreValuesSourceType.BYTES, indexFieldData.getValuesSourceType());
    }

    public void testLoad() throws IOException {
        final DirectoryReader reader = DirectoryReader.open(directory);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        KNNVectorDVLeafFieldData leafFieldData = indexFieldData.load(leafReaderContext);
        assertNotNull(leafFieldData);
        reader.close();
    }

    public void testLoadDirect() throws IOException {
        final DirectoryReader reader = DirectoryReader.open(directory);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        KNNVectorDVLeafFieldData leafFieldData = indexFieldData.loadDirect(leafReaderContext);
        assertNotNull(leafFieldData);
        reader.close();
    }

    public void testSortField() {

        expectThrows(UnsupportedOperationException.class,
                () -> indexFieldData.sortField(null, null, null, false));
    }

    public void testNewBucketedSort() {

        expectThrows(UnsupportedOperationException.class,
                () -> indexFieldData.newBucketedSort(null, null, null, null, null, null, 0, null));
    }
}
