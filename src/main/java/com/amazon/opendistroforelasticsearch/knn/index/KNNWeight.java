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

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import com.amazon.opendistroforelasticsearch.knn.index.util.KNNConstants;
import com.amazon.opendistroforelasticsearch.knn.index.v1736.KNNIndex;
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
import java.security.AccessController;
import java.security.PrivilegedAction;
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
        try {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(context.reader());
            String directory = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory().toString();

            /**
             * In case of compound file, extension would be .hnswc otherwise .hnsw
             */
            String hnswFileExtension = reader.getSegmentInfo().info.getUseCompoundFile()
                                               ? KNNCodecUtil.HNSW_COMPOUND_EXTENSION : KNNCodecUtil.HNSW_EXTENSION;
            String hnswSuffix = knnQuery.getField() + hnswFileExtension;
            List<String> hnswFiles = reader.getSegmentInfo().files().stream()
                                           .filter(fileName -> fileName.endsWith(hnswSuffix))
                                          .collect(Collectors.toList());

            if(hnswFiles.isEmpty()) {
                logger.debug("[KNN] No hsnw index found for field {} for segment {}",
                        knnQuery.getField(), reader.getSegmentName());
                return null;
            }

            FieldInfo queryFieldInfo = reader.getFieldInfos().fieldInfo(knnQuery.getField());

            /**
             * TODO Add logic to pick up the right nmslib version based on the version
             * in the name of the file. As of now we have one version 1.7.3.6.
             * So deferring this to future releases
             */

            Path indexPath = PathUtils.get(directory, hnswFiles.get(0));
            KNNQueryResult[] results = AccessController.doPrivileged(
                    new PrivilegedAction<KNNQueryResult[]>() {
                        public KNNQueryResult[] run() {
                            KNNIndex index = knnIndexCache.getIndex(indexPath.toString());
                            if(index.isDeleted.get()) {
                                // Race condition occured. Looks like entry got evicted from cache and
                                // possibly gc. Try to read again
                                logger.info("[KNN] Race condition occured. Looks like entry got evicted " +
                                                    "from cache and possible gc. Trying to read again");
                                index = knnIndexCache.getIndex(indexPath.toString());
                                if(index.isDeleted.get()) {
                                    logger.info("Index deleted. Possibly getting evicted as segment exceeds the cache max weight. Path: " + indexPath.toString());
                                    return  null;
                                }
                            }
                            return index.queryIndex(knnQuery.getQueryVector(), knnQuery.getK(), getQueryParams(queryFieldInfo));
                        }
                    }
            );

            if (results == null) {
                logger.debug("No results for field {} for segment {}",
                        knnQuery.getField(), reader.getSegmentName());
                return  null;
            }

            /**
             * Scores represent the distance of the documents with respect to given query vector.
             * Lesser the score, the closer the document is to the query vector.
             * Since by default results are retrieved in the descending order of scores, to get the nearest
             * neighbors we are inverting the scores.
             */
            Map<Integer, Float> scores = Arrays.stream(results).collect(
                    Collectors.toMap(result -> result.getId(), result -> 1/(1 + result.getScore())));
            int maxDoc = Collections.max(scores.keySet()) + 1;
            DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(maxDoc);
            DocIdSetBuilder.BulkAdder setAdder = docIdSetBuilder.grow(maxDoc);
            Arrays.stream(results).forEach(result -> setAdder.add(result.getId()));
            DocIdSetIterator docIdSetIter = docIdSetBuilder.build().iterator();
            return new KNNScorer(this, docIdSetIter, scores, boost);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCacheable(LeafReaderContext context) {
        return true;
    }

    private String[] getQueryParams(FieldInfo fieldInfo) {
        if (fieldInfo.attributes().containsKey(KNNConstants.HNSW_ALGO_EF_SEARCH)) {
            return new String[] {"efSearch=" + fieldInfo.attributes().get(KNNConstants.HNSW_ALGO_EF_SEARCH)};
        }
        return new String[] {};
    }
}
