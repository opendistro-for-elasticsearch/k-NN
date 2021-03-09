/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistroforelasticsearch.knn.common.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.index.faiss.v165.KNNFaissIndex;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;
import com.amazon.opendistroforelasticsearch.knn.index.nmslib.v2011.KNNNmsLibIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Calculate query weights and build query scorers.
 */
public class KNNWeight extends Weight {
    private static Logger logger = LogManager.getLogger(KNNWeight.class);
    private final KNNQuery knnQuery;
    private final float boost;

    public static KNNIndexCache knnIndexCache = KNNIndexCache.getInstance();

    public KNNWeight(KNNQuery query, float boost) {
        super(query);
        this.knnQuery = query;
        this.boost = boost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
        return Explanation.match(1.0f, "No Explanation");
    }

    @Override
    public void extractTerms(Set<Term> terms) {
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(context.reader());
            String directory = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory().toString();

            FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(knnQuery.getField());

            if (fieldInfo == null) {
                logger.debug("[KNN] Field info not found for {}:{}", knnQuery.getField(),
                        reader.getSegmentName());
                return null;
            }

            KNNEngine knnEngine = KNNEngine.getEngine(fieldInfo.getAttribute(KNNConstants.KNNEngine));
            logger.debug("[KNN] knnEngine for " + knnQuery.getField() + ": " + knnEngine.getKnnEngineName());

            String fileExtension = reader.getSegmentInfo().info.getUseCompoundFile()
                                               ? knnEngine.getCompoundExtension() : knnEngine.getExtension();
            String suffix = knnQuery.getField() + fileExtension;
            List<String> engineFiles = reader.getSegmentInfo().files().stream()
                    .filter(fileName -> fileName.endsWith(suffix))
                    .collect(Collectors.toList());

            if(engineFiles.isEmpty()) {
                logger.debug("[KNN] No engine index found for field {} for segment {}",
                        knnQuery.getField(), reader.getSegmentName());
                return null;
            }

            FieldInfo queryFieldInfo = reader.getFieldInfos().fieldInfo(knnQuery.getField());
            Map<String, String> fieldAttributes = queryFieldInfo.attributes();

            Path indexPath = PathUtils.get(directory, engineFiles.get(0));
            final KNNQueryResult[] results;
            final KNNIndex index = knnIndexCache.getIndex(indexPath.toString(), knnQuery.getIndexName());

            if ((fieldAttributes.containsValue(KNNEngine.NMSLIB.getKnnEngineName()) && index instanceof KNNNmsLibIndex)
                    || (fieldAttributes.containsValue(KNNEngine.FAISS.getKnnEngineName())
                    && index instanceof KNNFaissIndex)) {
                results = index.queryIndex(
                        knnQuery.getQueryVector(),
                        knnQuery.getK()
                );
            } else {
                throw new IllegalStateException("Unable to retrieve k-NN engine for index path: "
                        + indexPath.toString());
            }

            /*
             * Scores represent the distance of the documents with respect to given query vector.
             * Lesser the score, the closer the document is to the query vector.
             * Since by default results are retrieved in the descending order of scores, to get the nearest
             * neighbors we are inverting the scores.
             */
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(KNNQueryResult::getId, result -> normalizeScore(result.getScore())));
            int maxDoc = Collections.max(scores.keySet()) + 1;
            DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
            DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);
            Arrays.stream(results).forEach(result -> setAdder.add(result.getId()));
            DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
            return new KNNScorer(this, docIdSetIter, scores, boost);
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }

    public static float normalizeScore(float score) {
        if (score >= 0)
            return 1 / (1 + score);
        return 2 + ( 1 / (score - 1) );
    }
}
