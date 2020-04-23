# Open Distro for Elasticsearch KNN

Open Distro for Elasticsearch enables you to run nearest neighbor search on billions of documents across thousands of dimensions with the same ease as running any regular Elasticsearch query. You can use aggregations and filter clauses to further refine your similarity search operations. K-NN similarity search power use cases such as product recommendations, fraud detection, image and video search, related document search, and more.

## REQUEST FOR COMMENT (RFC)

We'd like to get your comments! Please read the plugin RFC [document](https://github.com/opendistro-for-elasticsearch/k-NN/blob/development/RFC.md) and raise an issue to add your comments and questions.

## Documentation

To learn more, please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/).

## Setup

1. Check out the package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.
3. To build from the command line, set `JAVA_HOME` to point to a JDK >= 12 before running `./gradlew`.

## Build

The package uses the [Gradle](https://docs.gradle.org/5.5.1/userguide/userguide.html) build system.

1. Checkout this package from version control.
2. To build from command line set `JAVA_HOME` to point to a JDK >=12
3. Run `./gradlew build`

### Debugging

Sometimes it's useful to attach a debugger to either the Elasticsearch cluster or the integ tests to see what's going on. When running unit tests, hit **Debug** from the IDE's gutter to debug the tests.  To debug code running in an actual server, run:

```
./gradlew :integTest --debug-jvm # to start a cluster and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The Elasticsearch server JVM will launch suspended and wait for a debugger to attach to `localhost:8000` before starting the Elasticsearch server.

To debug code running in an integ test (which exercises the server from a separate JVM), run:

```
./gradlew -Dtest.debug=1 integTest
```

The test runner JVM will start suspended and wait for a debugger to attach to `localhost:5005` before running the tests

## Basic Usage

* Creating KNN index
You can create a KNN type index by specifying codec as *KNNCodec* and mark the particular field as of type ‘*knn_vector’*.
The following code creates a KNN index with fields my_vector1, my_vector2, my_vector3 as knn types. The knn type field accepts array of float.

```
PUT /myindex
{
  "settings" : {
    "index": {
      "knn": true
    }
  },
  "mappings": {
    "my_images": {
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

* Indexing sample docs to KNN index

```
curl -X POST "localhost:9200/myindex/_doc/1" -H 'Content-Type: application/json' -d'
{
"my_vector" : [1.5, 2.5],
"price":10
}
'

curl -X PUT "localhost:9200/myindex/_doc/2" -H 'Content-Type: application/json' -d'
{
"my_vector" : [2.5, 3.5],
"price":12
}
'


curl -X PUT "localhost:9200/myindex/_doc/3" -H 'Content-Type: application/json' -d'
{
"my_vector" : [3.5, 4.5],
"price":15
}
'


curl -X PUT "localhost:9200/myindex/_doc/4" -H 'Content-Type: application/json' -d'
{
"my_vector" : [5.5, 6.5],
"price":17
}
'


curl -X PUT "localhost:9200/myindex/_doc/5" -H 'Content-Type: application/json' -d'
{
"my_vector" : [4.5, 5.5],
"price":19
}

#### graph_memory_usage
The current weight of the cache (the total size in native memory of all of the graphs) in Kilobytes.

#### graph_index_requests
The number of requests to add the knn_vector field of a document into a graph.

#### graph_index_errors
The number of requests to add the knn_vector field of a document into a graph that have produced an error.

#### graph_query_requests
The number of graph queries that have been made. 

#### graph_query_errors
The number of graph queries that have produced an error.

#### knn_query_requests
The number of KNN query requests received. 

#### cache_capacity_reached
Whether the cache capacity for this node has been reached. This capacity can be controlled as part of the *knn.memory.circuit_breaker.limit.*

#### load_exception_count
The number of exceptions that have occurred when trying to load an item into the cache. This count could increase when graph loading has exceptions.

#### load_success_count
The number of times an item is successfully loaded into the cache.

#### total_load_time
The total time in nanoseconds it has taken to load items into cache (cumulative).

#### Examples
```

GET /_opendistro/_knn/stats?pretty
{
    "_nodes" : {
        "total" : 1,
        "successful" : 1,
        "failed" : 0
    },
    "cluster_name" : "_run",
    "circuit_breaker_triggered" : false,
    "nodes" : {
        "HYMrXXsBSamUkcAjhjeN0w" : {
            "eviction_count" : 0,
            "miss_count" : 1,
            "graph_memory_usage" : 1,
            "graph_index_requests" : 7,
            "graph_index_errors" : 1,
            "knn_query_requests" : 4,
            "graph_query_requests" : 30,
            "graph_query_errors" : 15,
            "cache_capacity_reached" : false,
            "load_exception_count" : 0,
            "hit_count" : 0,
            "load_success_count" : 1,
            "total_load_time" : 2878745
        }
    }
}
'
```

* Querying K-Nearest neighbors

```
curl -X POST "localhost:9200/myindex/_search" -H 'Content-Type: application/json' -d'
{"size" : 10,
 "query": {
  "knn": {
   "my_vector": {
     "vector": [3, 4],
     "k": 2
   }
  }
 }
}
'
```

## Java Native library usage
For plugin installations from archive(.zip), it is necessary to ensure ```.so``` file for linux OS and ```.jnilib``` file for Mac OS are present in the java library path. This can be possible by copying .so/.jnilib to either $ES_HOME or by adding manually ```-Djava.library.path=<path_to_lib_files>``` in ```jvm.options``` file

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
