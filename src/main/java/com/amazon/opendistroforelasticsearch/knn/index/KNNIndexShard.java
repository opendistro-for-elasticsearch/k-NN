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

import com.amazon.opendistroforelasticsearch.knn.index.codec.KNNCodecUtil;
import com.amazon.opendistroforelasticsearch.knn.index.v1736.KNNIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.amazon.opendistroforelasticsearch.knn.index.KNNSettings.KNN_INDEX;

/**
 * KNNIndexShard wraps indexShard and adds methods to perform KNN related operations against the shard
 */
public class KNNIndexShard {
    private IndexShard indexShard;
    private KNNIndexCache knnIndexCache;

    private static Logger logger = LogManager.getLogger(KNNIndexShard.class);

    public static class NotKNNIndexException extends Exception {
        public NotKNNIndexException(String errorMessage) {
            super(errorMessage + " The index does not have index.knn setting set to true.");
        }
    }

    public KNNIndexShard(IndexShard indexShard)
            throws NotKNNIndexException {
        if (!"true".equals(indexShard.indexSettings().getSettings().get(KNN_INDEX))) {
            throw new NotKNNIndexException("[KNN] Exception validating " + indexShard.shardId().getIndexName() + ".");
        }

        this.indexShard = indexShard;
        this.knnIndexCache = KNNIndexCache.getInstance();
    }

    public IndexShard getIndexShard() {
        return indexShard;
    }

    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    public List<KNNIndex> warmup() throws IOException {
        logger.info("[KNN] Warming up index: " + getIndexName());
        Engine.Searcher searcher = indexShard.acquireSearcher("knn-warmup");
        List<KNNIndex> indices;
        try {
            indices = knnIndexCache.getIndices(getHNSWPaths(searcher.getIndexReader()), getIndexName());
            searcher.close();
        } catch (IOException ex) {
            searcher.close();
            throw ex;
        }
        return indices;
    }

    public List<String> getHNSWPaths(IndexReader indexReader) throws IOException {
        List<String> hnswFiles = new ArrayList<>();
        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(leafReaderContext.reader());
            hnswFiles.addAll(reader.getSegmentInfo().files().stream()
                    .filter(fileName -> fileName.endsWith(getHNSWFileExtension(reader.getSegmentInfo().info)))
                    .map(fileName -> shardPath().resolveIndex().resolve(fileName).toString())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
        }
        return hnswFiles;
    }

    private ShardPath shardPath() {
        return indexShard.shardPath();
    }

    private String getHNSWFileExtension(SegmentInfo info) {
        return info.getUseCompoundFile() ? KNNCodecUtil.HNSW_COMPOUND_EXTENSION : KNNCodecUtil.HNSW_EXTENSION;
    }
}
