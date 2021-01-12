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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.junit.Before;

import java.io.IOException;

public class KNNVectorDVLeafFieldDataTests extends KNNTestCase {

    private static final String MOCK_INDEX_FIELD_NAME = "test-index-field-name";
    private static final String MOCK_NUMERIC_INDEX_FIELD_NAME = "test-index-price";
    private LeafReaderContext leafReaderContext;
    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        createKNNVectorDocument(directory);
        reader = DirectoryReader.open(directory);
        leafReaderContext = reader.getContext().leaves().get(0);
    }

    private void createKNNVectorDocument(Directory directory) throws IOException {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, conf);
        Document knnDocument = new Document();
        knnDocument.add(
                new BinaryDocValuesField(
                        MOCK_INDEX_FIELD_NAME,
                        new VectorField(MOCK_INDEX_FIELD_NAME, new float[]{1.0f, 2.0f}, new FieldType()).binaryValue()));
        knnDocument.add(new NumericDocValuesField(MOCK_NUMERIC_INDEX_FIELD_NAME, 1000));
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

    public void testGetScriptValues() {
        KNNVectorDVLeafFieldData leafFieldData = new KNNVectorDVLeafFieldData(leafReaderContext.reader(), MOCK_INDEX_FIELD_NAME);
        ScriptDocValues<float[]> scriptValues = leafFieldData.getScriptValues();
        assertNotNull(scriptValues);
        assertTrue(scriptValues instanceof KNNVectorScriptDocValues);
    }

    public void testGetScriptValuesWrongFieldName() {
        KNNVectorDVLeafFieldData leafFieldData = new KNNVectorDVLeafFieldData(
                leafReaderContext.reader(), "invalid");
        ScriptDocValues<float[]> scriptValues = leafFieldData.getScriptValues();
        assertNotNull(scriptValues);
    }

    public void testGetScriptValuesWrongFieldType() {
        KNNVectorDVLeafFieldData leafFieldData = new KNNVectorDVLeafFieldData(
                leafReaderContext.reader(), MOCK_NUMERIC_INDEX_FIELD_NAME);
        expectThrows(IllegalStateException.class, ()->leafFieldData.getScriptValues());
    }

    public void testRamBytesUsed() {
        KNNVectorDVLeafFieldData leafFieldData = new KNNVectorDVLeafFieldData(leafReaderContext.reader(), "");
        assertEquals(0, leafFieldData.ramBytesUsed());
    }

    public void testGetBytesValues() {
        KNNVectorDVLeafFieldData leafFieldData = new KNNVectorDVLeafFieldData(leafReaderContext.reader(), "");
        expectThrows(UnsupportedOperationException.class,
                () -> leafFieldData.getBytesValues());
    }
}
