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

package com.amazon.opendistroforelasticsearch.knn.plugin.scripts;

import com.amazon.opendistroforelasticsearch.knn.KNNTestCase;
import com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNLongScoreScript;
import com.amazon.opendistroforelasticsearch.knn.plugin.script.KNNScoringUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KNNLongScoreScriptTests extends KNNTestCase {

    public void testKNNLongScoreScript_Hamming() throws IOException {
        String fieldName = "test-field";
        Directory dir = newFSDirectory(createTempDir());
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
        Document doc = new Document();
        NumericDocValuesField numericDocValuesField = new NumericDocValuesField(fieldName, 10L);
        doc.add(numericDocValuesField);
        writer.addDocument(doc);
        writer.commit();

        IndexReader reader = DirectoryReader.open(writer);
        LeafReaderContext leafReaderContext = reader.getContext().leaves().iterator().next();

        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);



        KNNLongScoreScript knnLongScoreScript = new KNNLongScoreScript(new HashMap<>(), fieldName, 10L,
                KNNScoringUtil::bitHamming, lookup, leafReaderContext);

        assertEquals(1.0, knnLongScoreScript.execute(new ScoreScript.ExplanationHolder()), 0.1);
    }
}
