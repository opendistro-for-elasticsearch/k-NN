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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Class for representing the KNN query
 */
public class KNNQuery extends Query {

    private final String field;
    private final float[] queryVector;
    private final int k;
    private final String indexName;

    public KNNQuery(String field, float[] queryVector, int k, String indexName) {
        this.field = field;
        this.queryVector = queryVector;
        this.k = k;
        this.indexName = indexName;
    }

    public String getField() {
        return this.field;
    }

    public float[] getQueryVector() {
        return this.queryVector;
    }

    public int getK() {
        return this.k;
    }

    public String getIndexName() { return this.indexName; }

    /**
     * Constructs Weight implementation for this query
     *
     * @param searcher  searcher for given segment
     * @param scoreMode  How the produced scorers will be consumed.
     * @param boost     The boost that is propagated by the parent queries.
     * @return Weight   For calculating scores
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (!KNNSettings.isKNNPluginEnabled()) {
            throw new IllegalStateException("KNN plugin is disabled. To enable update knn.plugin.enabled to true");
        }
        return new KNNWeight(this, boost);
    }

    @Override
    public String toString(String field) {
        return field;
    }

    @Override
    public int hashCode() {
        return field.hashCode() ^ queryVector.hashCode() ^ k;
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
                       equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(KNNQuery other) {
        return this.field.equals(other.getField()) && this.queryVector.equals(other.getQueryVector()) && this.k == other.getK();
    }
};
