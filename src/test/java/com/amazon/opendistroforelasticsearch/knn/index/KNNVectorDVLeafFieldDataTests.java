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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KNNVectorDVLeafFieldDataTests extends KNNTestCase {

    private static final String MOCK_INDEX_FIELD_NAME = "test-index-field-name";
    private KNNVectorDVLeafFieldData leafFieldData;
    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        createKNNVectorDocument(directory);
        reader = DirectoryReader.open(directory);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        leafFieldData = new KNNVectorDVLeafFieldData(leafReaderContext.reader(), MOCK_INDEX_FIELD_NAME);
    }

    private void createKNNVectorDocument(Directory directory) throws IOException {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, conf);
        Document knnDocument = new Document();
        knnDocument.add(
                new BinaryDocValuesField(
                        MOCK_INDEX_FIELD_NAME,
                        new VectorField(MOCK_INDEX_FIELD_NAME, new float[]{1.0f, 2.0f}, new FieldType()).binaryValue()));
        writer.addDocument(knnDocument);
        writer.commit();
        writer.close();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        directory.close();
    }

    @Test
    public void testGetScriptValues() {
        ScriptDocValues<float[]> scriptValues = leafFieldData.getScriptValues();
        assertNotNull(scriptValues);
        assertTrue(scriptValues instanceof KNNVectorScriptDocValues);
    }
}