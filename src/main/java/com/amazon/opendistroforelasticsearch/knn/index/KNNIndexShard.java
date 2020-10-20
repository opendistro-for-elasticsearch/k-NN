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
import com.amazon.opendistroforelasticsearch.knn.index.v206.KNNIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * KNNIndexShard wraps IndexShard and adds methods to perform k-NN related operations against the shard
 */
public class KNNIndexShard {
    private IndexShard indexShard;
    private KNNIndexCache knnIndexCache;

    private static Logger logger = LogManager.getLogger(KNNIndexShard.class);

    /**
     * Constructor to generate KNNIndexShard. We do not perform validation that the index the shard is from
     * is in fact a k-NN Index (index.knn = true). This may make sense to add later, but for now the operations for
     * KNNIndexShards that are not from a k-NN index should be no-ops.
     *
     * @param indexShard IndexShard to be wrapped.
     */
    public KNNIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.knnIndexCache = KNNIndexCache.getInstance();
    }

    /**
     * Return the underlying IndexShard
     *
     * @return IndexShard
     */
    public IndexShard getIndexShard() {
        return indexShard;
    }

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    /**
     * Load all of the HNSW graphs for this shard into the cache. Note that getIndices is called to prevent loading
     * in duplicates.
     *
     * @return a List of KNNIndex's from this shard that are in the cache after this operation.
     * @throws IOException Thrown when getting the HNSW Paths to be loaded in
     */
    public List<KNNIndex> warmup() throws IOException {
        logger.info("[KNN] Warming up index: " + getIndexName());
        Engine.Searcher searcher = indexShard.acquireSearcher("knn-warmup");
        List<KNNIndex> indices;
        try {
            indices = knnIndexCache.getIndices(getHNSWPaths(searcher.getIndexReader()), getIndexName());
        } finally {
            searcher.close();
        }
        return indices;
    }

    /**
     * For the given shard, get all of its HNSW paths
     *
     * @param indexReader IndexReader to read the file paths for the shard
     * @return List of HNSW Paths
     * @throws IOException Thrown when the SegmentReader is attempting to read the segments files
     */
    public List<String> getHNSWPaths(IndexReader indexReader) throws IOException {
        List<String> hnswFiles = new ArrayList<>();
        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = (SegmentReader) FilterLeafReader.unwrap(leafReaderContext.reader());
            Path shardPath = ((FSDirectory) FilterDirectory.unwrap(reader.directory())).getDirectory();
            hnswFiles.addAll(reader.getSegmentInfo().files().stream()
                    .filter(fileName -> fileName.endsWith(getHNSWFileExtension(reader.getSegmentInfo().info)))
                    .map(fileName -> shardPath.resolve(fileName).toString())
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
