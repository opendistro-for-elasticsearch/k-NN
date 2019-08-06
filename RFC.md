# K-Nearest Neighbors(KNN) Search Plugin

The purpose of this document is to request the community for comments (RFC) for a new Open Distro for Elasticsearch KNN plugin and to collect feedback from the community. This RFC covers the high-level functionality of the KNN plugin and does not cover architecture and implementation details.

## Problem Statement
Customers with similarity-based search use cases such as face search, fraud detection and recommendation systems have a need for finding K-Nearest Neighbor (KNN) using the machine learning models. The existing solutions for KNN search were inefficient and did not operate well at scale. Brute-force KNN search using CPU is very computationally intensive and generally too slow for large-scale real-time search applications.

## Proposed Solution

Open Distro for Elasticsearch enables you to run nearest neighbor search on billions of documents across thousands of dimensions with the same ease as running any regular Elasticsearch query. You can use aggregations and filter clauses to further refine your similarity search operations. Power use-cases include product recommendations, fraud detection, image and video search, related document search, and more.

Open Distro for Elasticsearch uses the Non-Metric Space Library (NMSLIB), a highly efficient implementation of k-NN, which has consistently out-performed most of the other solutions as per the ANN-Benchmarks published here. The solution has extended an Apache Lucene codec to introduce a separate file format for storing k-NN indices to deliver high efficiency k-NN search operations on Elasticsearch.

* Essentially, the KNN feature is powered by 4 customizations to Elasticsearch:
    * [Mapper plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper.html) to support new field type, ```knn_vector``` to represent the vector fields in a document.

    * [Lucene Codec](https://www.elastic.co/blog/what-is-an-apache-lucene-codec) named ‘KNNCodec’ which adds a new Lucene index file format for storing and retrieving the vectors in nmslib using a JNI layer.

    * [Search plugin](https://static.javadoc.io/org.elasticsearch/elasticsearch/7.2.0/org/elasticsearch/plugins/SearchPlugin.html) which introduces a query clause called ```knn``` for processing the KNN query elements.

    * [Action plugin](https://static.javadoc.io/org.elasticsearch/elasticsearch/7.2.0/org/elasticsearch/plugins/ActionPlugin.html) to utilize Elasticsearch ResourceWatcher service for effective garbage collection management of hnsw indices.

####Highly scalable

This plugin will enable the user to easily leverage Elasticsearch’s distributed architecture to run high-scale k-NN search operations.  Unlike many of the common k-NN solutions, this Open Distro for Elasticsearch plugin does not plan to use a brute-force approach to compute k-NN during a search operation. This approach could cause exponential degradation in performance with scale. Instead, the solution indexes the k-NN data efficiently, enabling the user to attain low latency even at high scale.

####Run k-NN using familiar Elasticsearch constructs

k-NN on Open Distro for Elasticsearch uses the familiar mapping and query syntax of Elasticsearch. To designate a field as a k-NN vector, you simply need to map it to the new k-NN field type provided by the mapper plugin. You can then invoke k-NN search operations using the search plugin with the familiar Elasticsearch query syntax.

####Combine with other Elasticsearch features

k-NN functionality integrates seamlessly with other Elasticsearch features. This provides you the flexibility to use Elasticsearch’s extensive search features such as aggregations and filtering with k-NN to further slice and dice your data to increase the precision of your searches.

## Usage

KNN indices need ```KNNCodec``` to write and read hnsw indices created part of each segment for a shard. Please find below example to create knn index.

### Creating K-NN index
``` JSON
PUT /myindex
{
  "settings": {
    "index": {
      "codec": "KNNCodec"
    }
  },
  "mappings": {
    "properties": {
      "my_vector1": {
        "type": "knn_vector"
      },
      "my_vector2": {
        "type": "knn_vector"
      },
      "my_vector3": {
        "type": "knn_vector"
      }
    }
  }
}
```
In the above example, we are creating K-NN index with 3 ```knn_vector``` fields namely my_vector1, my_vector2, my_vector3. We could index different category of embedding into these fields and query the nearest neighbors for each field independently.

### Indexing vectors
``` JSON
POST my_index/_doc/1
{
  "my_vector1" : [1.5, 2.5]
}


POST my_index/_doc/2
{
  "my_vector1" : [2.5, 3.5]
}


POST my_index/_doc/3
{
  "my_vector1" : [4.5, 5.5]
}


POST my_index/_doc/4
{
  "my_vector2" : [6.5, 7.5, 8.5]
}

POST my_index/_doc/5
{
  "my_vector2" : [8.5, 9.5, 0.5]
}

POST my_index/_doc/6
{
  "my_vector3" : [8.5, 9.5, 10.5, 14.5, 20.19, 22.42]
}
```

In the above examples, we have indexed 3 vectors of field ```my_vector1```, 2 vectors of field ```my_vector2```, 1 vector of field ```my_vector3```

### Indexing different category of embeddings in the same document

``` JSON
POST my_index/_doc/7
{
  "my_vector1" : [2.5, 4.5],
  "my_vector2" : [13.5, 14.5, 15.5],
  "my_vector3" : [8.5, 9.5, 10.5, 7,8]
}
```

### Using knn vector fields in combination with other fields

``` JSON
POST my_index/_doc/8
{
  "my_vector2" : [8.5, 9.5, 0.5],
  "price": 10
}
```


### Querying K-Nearest Neighbors
Use the new ```knn``` clause in the query DSL and specify the point of interest as ‘vector’ and the number of
nearest neighbors to fetch as ```k```.

``` JSON
POST localhost:9200/myindex/_search
{
 “size”: 2,
 "query": {
  "knn": {
   "my_vector1": {
     "vector": [3, 4],
     "k": 2
   }
  }
 }
}
```
#### Output for above query

``` JSON
{
  "took": 967067,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 4,
      "relation": "eq"
    },
    "max_score": 1.4142135,
    "hits": [
      {
        "_index": "myindex",
        "_type": "_doc",
        "_id": "2",
        "_score": 1.4142135,
        "_source": {
          "my_vector1": [
            2.5,
            3.5
          ]
        }
      },
      {
        "_index": "myindex",
        "_type": "_doc",
        "_id": "7",
        "_score": 1.4142135,
        "_source": {
          "my_vector1": [
            2.5,
            4.5
          ]
        }
      }
    ]
  }
}
```

## Providing Feedback

If you have any comments or feedback on the proposed design for the K-NN plugin, please comment on [the RFC Github issue](../../issues/1) on this project to discuss.
