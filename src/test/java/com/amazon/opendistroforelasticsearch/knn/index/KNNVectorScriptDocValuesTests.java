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
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;

public class KNNVectorScriptDocValuesTests extends KNNTestCase {

    private static final String MOCK_INDEX_FIELD_NAME = "test-index-field-name";
    private static final float[] SAMPLE_VECTOR_DATA = new float[]{1.0f, 2.0f};
    private KNNVectorScriptDocValues scriptDocValues;
    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        createKNNVectorDocument(directory);
        reader = DirectoryReader.open(directory);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().get(0);
        scriptDocValues = new KNNVectorScriptDocValues(
                leafReaderContext.reader().getBinaryDocValues(MOCK_INDEX_FIELD_NAME), MOCK_INDEX_FIELD_NAME);
    }

    private void createKNNVectorDocument(Directory directory) throws IOException {
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, conf);
        Document knnDocument = new Document();
        knnDocument.add(
                new BinaryDocValuesField(
                        MOCK_INDEX_FIELD_NAME,
                        new VectorField(MOCK_INDEX_FIELD_NAME, SAMPLE_VECTOR_DATA, new FieldType()).binaryValue()));
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

    public void testGetValue() throws IOException {
        scriptDocValues.setNextDocId(0);
        Assert.assertArrayEquals(SAMPLE_VECTOR_DATA, scriptDocValues.getValue(), 0.1f);
    }


    //Test getValue without calling setNextDocId
    public void testGetValueFails() throws IOException {
        expectThrows(IllegalStateException.class, () -> scriptDocValues.getValue());
    }

    public void testSize() throws IOException {
        Assert.assertEquals(0, scriptDocValues.size());
        scriptDocValues.setNextDocId(0);
        Assert.assertEquals(1, scriptDocValues.size());
    }

    public void testGet() throws IOException {
        expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
    }
}
