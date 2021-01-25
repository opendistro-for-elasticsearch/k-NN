package com.amazon.opendistroforelasticsearch.knn.index;


import com.amazon.opendistroforelasticsearch.knn.index.util.KNNEngine;

public abstract class KNNIndex implements AutoCloseable{


    public KNNIndex() {}

    /**
     * Query the KNN Index
     * @param query query vector
     * @param k top k nearest
     * @return array of @KNNQueryResult
     */
    public abstract KNNQueryResult[] queryIndex(final float[] query, final int k);


    /**
     * Save the data into knn
     * @param ids ids
     * @param data data
     * @param indexPath path to save
     * @param algoParams algorithms params passed to KNN
     * @param spaceType space type for this index
     * @param engine verify engine
     */
    public abstract void saveIndex(int[] ids, float[][] data, String indexPath, String[] algoParams, String spaceType, KNNEngine engine);


    /**
     * This function is useful in computing the weight for caching. File sizes are stored in KiloBytes to prevent an
     * Integer Overflow. The Guava Cache weigh method returns an int. The max size of a Java int is 2,147,483,647. So,
     * a 2GB file, would lead to an overflow. With KB, however, 2,147,483,647 KB = 1.99 TB. So, it would take a 2 TB
     * file to produce an Integer Overflow.
     *
     * @return size of the hnsw index on the disk in KB.
     */
    public abstract long getIndexSize();

    public abstract void close();
}
