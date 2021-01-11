[![Testing Workflow](https://github.com/opendistro-for-elasticsearch/k-NN/workflows/Testing%20Workflow/badge.svg)](https://github.com/opendistro-for-elasticsearch/k-NN/actions)
[![codecov](https://codecov.io/gh/opendistro-for-elasticsearch/k-NN/branch/master/graph/badge.svg)](https://codecov.io/gh/opendistro-for-elasticsearch/k-NN)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://opendistro.github.io/for-elasticsearch-docs/docs/knn/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/k-NN/)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

# Open Distro for Elasticsearch KNN

Open Distro for Elasticsearch enables you to run nearest neighbor search on billions of documents across thousands of dimensions with the same ease as running any regular Elasticsearch query. You can use aggregations and filter clauses to further refine your similarity search operations. K-NN similarity search power use cases such as product recommendations, fraud detection, image and video search, related document search, and more.

## Documentation

To learn more, please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/docs/knn).

## Setup

1. Check out the package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.
3. To build from the command line, set `JAVA_HOME` to point to a JDK 14 before running `./gradlew`.

## Build

The package uses the [Gradle](https://docs.gradle.org/5.5.1/userguide/userguide.html) build system.

1. Checkout this package from version control.
2. To build from command line set `JAVA_HOME` to point to a JDK >=13
3. Run `./gradlew build`

## Building JNI Library

To build the JNI Library used to incorporate NMSLIB functionality, follow these steps:

```
cd jni
cmake .
make
```

The library will be placed in the `jni/release` directory.

## JNI Library Artifacts
We build and distribute binary library artifacts with Opendistro for Elasticsearch. We build the library binary, RPM and DEB in [this GitHub action](https://github.com/opendistro-for-elasticsearch/k-NN/blob/master/.github/workflows/CD.yml). We use Centos 7 with g++ 4.8.5 to build the DEB, RPM and ZIP. Additionally, in order to provide as much general compatibility as possible, we compile the library without optimized instruction sets enabled. For users that want to get the most out of the library, they should follow [this section](##Build JNI Library RPM/DEB) and build the library from source in their production environment, so that if their environment has optimized instruction sets, they take advantage of them.

## Build JNI Library RPM/DEB

To build an RPM or DEB of the JNI library, follow these steps:

```
cd jni
cmake .
make package
```

The artifacts will be placed in the `jni/packages` directory.



## Running Multi-node Cluster Locally

It can be useful to test and debug on a multi-node cluster. In order to launch a 3 node cluster with the KNN plugin installed, run the following command:

```
./gradlew run -PnumNodes=3
```

In order to run the integration tests with a 3 node cluster, run this command:

```
./gradlew :integTest -PnumNodes=3
```

### Debugging

Sometimes it is useful to attach a debugger to either the Elasticsearch cluster or the integration test runner to see what's going on. For running unit tests, hit **Debug** from the IDE's gutter to debug the tests. For the Elasticsearch cluster, first, make sure that the debugger is listening on port `5005`. Then, to debug the cluster code, run:

```
./gradlew :integTest -Dcluster.debug=1 # to start a cluster with debugger and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The Elasticsearch server JVM will connect to a debugger attached to `localhost:5005` before starting. If there are multiple nodes, the servers will connect to debuggers listening on ports `5005, 5006, ...`

To debug code running in an integration test (which exercises the server from a separate JVM), first, setup a remote debugger listening on port `8000`, and then run:

```
./gradlew :integTest -Dtest.debug=1
```

The test runner JVM will connect to a debugger attached to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :integTest -Dtest.debug=1 -Dcluster.debug=1
```

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
        "properties": {
            "my_vector1": {
                "type": "knn_vector",
                "dimension": 2
            },
            "my_vector2": {
                "type": "knn_vector",
                "dimension": 4
            },
            "my_vector3": {
                "type": "knn_vector",
                "dimension": 8
            }
        }
    }
}
```

* Indexing sample docs to KNN index

```
PUT /myindex/_doc/2?refresh=true
{
    "my_vector1" : [1.5, 2.5],
    "price":10
}
```

* Querying K-Nearest neighbors

```
POST /myindex/_search
{
    "size" : 10,
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

## Cosine Similarity Usage (experimental)

* Creating KNN index with cosine similarity space type

```
PUT /myindex
{
    "settings" : {
        "index": {
            "knn": true,
            "knn.space_type": "cosinesimil"
        }
    },
    "mappings": {
        "properties": {
            "my_vector1": {
                "type": "knn_vector",
                "dimension": 2
            }
        }
    }
}
```

* Indexing sample docs to KNN index

```
PUT /myindex/_doc/2?refresh=true
{
    "my_vector1" : [1.5, 2.5],
    "price":10
}
```

* Querying K-Nearest neighbors

```
POST /myindex/_search
{
    "size" : 10,
    "query": {
        "knn": {
            "my_vector1": {
                "vector": [15, 25],
                "k": 2
            }
        }
    }
}
```

## Java Native library usage
For plugin installations from archive(.zip), it is necessary to ensure ```.so``` file for linux OS and ```.jnilib``` file for Mac OS are present in the java library path. This can be possible by copying .so/.jnilib to either $ES_HOME or by adding manually ```-Djava.library.path=<path_to_lib_files>``` in ```jvm.options``` file

## Settings
### Index Level Settings
You must provide index-level settings when you create the index. If you don't provide these settings, KNN uses its default values. These settings are static, which means you can't modify them after index creation.

##### index.knn
This setting indicates whether the index uses the KNN Codec or not. Possible values are *true*, *false*. Default value is *false*.

##### index.knn.space_type
This setting indicates the similarity metrics between vectors. Supported values are *l2*, *cosinesimil*. *l2* refers to euclidean distance metric; *cosinesimil* refers to cosine similarity. Default value is *l2*.

##### index.knn.algo_param.m
This setting is an HNSW parameter that represents "the number of bi-directional links created for every new element during construction. Reasonable range for M is 2-100. Higher M work better on datasets with high intrinsic dimensionality and/or high recall, while low M work better for datasets with low intrinsic dimensionality and/or low recalls. The parameter also determines the algorithm's memory consumption, which is roughly M * 8-10 bytes per stored element." [nmslib/hnswlib](https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md) The default value is *16*.

##### index.knn.algo_param.ef_search
This setting is an HNSW parameter that represents "the size of the dynamic list for the nearest neighbors (used during the search). Higher ef leads to more accurate but slower search." [nmslib/hnswlib](https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md) The default value is *512*.

##### index.knn.algo_param.ef_construction
This setting is an HNSW parameter that "the parameter has the same meaning as ef, but controls the index_time/index_accuracy. Bigger ef_construction leads to longer construction, but better index quality." [nmslib/hnswlib](https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md) The default value is *512*.

##### Example
```
PUT /my_index/_settings
{
    "index" : {
        "knn": true,
        "knn.space_type": "l2",
        "knn.algo_param.m": 18, 
        "knn.algo_param.ef_search" : 20,
        "knn.algo_param.ef_construction" : 40
    }
}
```

### Cluster Level Settings
#### General
##### knn.plugin.enabled
This setting indicates whether or not the KNN Plugin is enabled. If it is disabled, a user will not be able to index knn_vector fields nor run KNN queries. The default value is *true.*

##### knn.algo_param.index_thread_qty
This setting specifies how many threads the NMS library should use to create the graph in memory. By default, the NMS library sets this value to the number of cores the machine has. However, because ES can spawn the same number of threads for searching, this could lead to (number of cores)^2 threads running and lead to 100% CPU utilization. The default value is *1.*

#### Cache
The KNN Plugin uses a Guava cache to keep track of the graphs currently loaded into native memory. When a query is run against a graph for the first time, the graph is loaded into native memory (outside the Java heap). Because Elasticsearch runs inside of the JVM, it cannot manage native memory directly. So, it keeps track of native memory by adding an entry into a Guava cache that contains the pointer to the graph in native memory and how much memory it uses.  The cache’s weight just means how much native memory all of the elements in the cache are taking up. If the maximum weight (this value is set by *knn.memory.circuit_breaker.limit*) of the cache is exceeded when it tries to load a graph into memory, the cache evicts an entry to make room for the new entry. Additionally, the cache can evict entries based on how long it has been since they were last accessed.

##### knn.cache.item.expiry.enabled
This setting indicates that the cache should evict entries that have expired (not been accessed for *knn.cache.item.expiry.minutes*). The default value is *false.*

##### knn.cache.item.expiry.minutes
This setting indicates how long an item can be in the cache without being accessed before it expires. When an entry expires, it gets evicted from the cache. The default value is *180 minutes.*

#### Circuit Breaker
For KNN, the circuit breaker is used to indicate when performance may degrade because the graphs loaded into native memory are reaching the cluster’s total limits. Currently, the system does not perform any action once this limit is reached.

##### knn.memory.circuit_breaker.enabled
This setting enables or disables the circuit breaker feature.  Disabling this setting will keep you at risk of Out of memory as we do not have control on the memory usage for the graphs. The default value is *true*.

##### knn.memory.circuit_breaker.limit
This setting indicates the maximum capacity of the cache. When the cache attempts to load in a graph that exceeds this limit, it is forced to evict an entry and *knn.circuit_breaker.triggered *is set to *true.* The default value for this setting is *60% *of the machines total memory outside the Elasticsearch jvm . However, a value in *KB* can be given as well.

###### Example
If a machine has 100GB RAM. Elasticsearch jvm uses 32GB. Then the default circuit breaker limit is set at 60% of the remaining memory(60% of (100GB -32GB) = 40.8GB)

##### knn.circuit_breaker.triggered
This setting indicates whether  or not the circuit breaker has been triggered. The circuit breaker is triggered if any node in the cluster has had to evict an entry from the cache because the cache’s capacity had been reached. The circuit breaker is untriggered when the size of the entries in the cache goes below *knn.circuit_breaker.unset.percentage. *This can occur when an index is deleted or entries in the cache expire when *knn.cache.item.expiry.enabled* is true. The default value is *False.*

##### knn.circuit_breaker.unset.percentage
This setting indicates under what percentage of the cache’s total capacity the cache’s current size must be in order to untrigger the circuit breaker. The default value is *75% *of total cache’s capacit*y.*

###### Example
```
PUT /_cluster/settings
{
    "persistent" : {
        "knn.plugin.enabled" : true,
        "knn.algo_param.index_thread_qty" : 1,
        "knn.cache.item.expiry.enabled": true,
        "knn.cache.item.expiry.minutes": "15m",
        "knn.memory.circuit_breaker.enabled" : true,
        "knn.memory.circuit_breaker.limit" : "55%",
        "knn.circuit_breaker.unset.percentage": 23
    }
}
```

## Stats
The KNN Stats API provides information about the current status of the KNN Plugin. The plugin keeps track of both cluster level and node level stats. Cluster level stats have a single value for the entire cluster. Node level stats have a single value for each node in the cluster. A user can filter their query by nodeID and statName in the following way:
```
GET /_opendistro/_knn/nodeId1,nodeId2/stats/statName1,statName2
```

### Cluster Stats
#### circuit_breaker_triggered
Indicates whether the circuit breaker is triggered.

### Node Stats
#### eviction_count
The number of evictions that have occurred in the guava cache. *note:* explicit evictions that occur because of index deletion are not counted.

#### hit_count
The number of cache hits that have occurred on the node. A cache hit occurs when a user queries a graph and it is already loaded into memory.

#### miss_count
The number of cache misses that have occurred on the node. A cache miss occurs when a user queries a graph and it has not yet been loaded into memory.

#### graph_memory_usage
The current weight of the cache (the total size in native memory of all of the graphs) in Kilobytes.

#### graph_memory_usage_percentage
The current weight of the cache as a percentage of the maximum cache capacity.

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

#### indices_in_cache
For each index that has graphs in the cache, this stat provides the number of graphs that index has and the total graph_memory_usage that index is using in Kilobytes.

#### script_compilations
The number of times the knn script is compiled. This value should only be 0 or 1 most of the time. However, if the cache containing the compiled scripts is filled, it may cause the script to be recompiled.

#### script_compilation_errors
The number of errors during script compilation.

#### script_query_requests
The number of query requests that use the k-NN score script. One query request corresponds to one query for a given shard.

#### script_query_errors
The number of errors that have occurred during use of the k-NN score script. One error corresponds to one error for a given a shard.

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
            "graph_memory_usage_percentage" : 3.68,
            "graph_index_requests" : 7,
            "graph_index_errors" : 1,
            "knn_query_requests" : 4,
            "graph_query_requests" : 30,
            "graph_query_errors" : 15,
            "indices_in_cache" : {
                "myindex" : {
                    "graph_memory_usage" : 2,
                    "graph_memory_usage_percentage" : 3.68,
                    "graph_count" : 2
                }
            },
            "cache_capacity_reached" : false,
            "load_exception_count" : 0,
            "hit_count" : 0,
            "load_success_count" : 1,
            "total_load_time" : 2878745,
            "script_compilations" : 1,
            "script_compilation_errors" : 0,
            "script_query_requests" : 534,
            "script_query_errors" : 0
        }
    }
}
```

```
GET /_opendistro/_knn/HYMrXXsBSamUkcAjhjeN0w/stats/circuit_breaker_triggered,graph_memory_usage?pretty
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
            "graph_memory_usage" : 1
        }
    }
}
```

## Warmup API
### Overview
The HNSW graphs used to perform k-Approximate Nearest Neighbor Search are stored as `.hnsw` files with the other Lucene segment files. In order to perform search on these graphs, they need to be loaded into native memory. If the graphs have not yet been loaded into native memory, upon search, they will first be loaded and then searched. This can cause high latency during initial queries. To avoid this, users will often run random queries during a warmup period. After this warmup period, the graphs will be loaded into native memory and their production workloads can begin. This process is indirect and requires extra effort. 

As an alternative, a user can run the warmup API on whatever indices they are interested in searching over. This API will load all the graphs for all of the shards (primaries and replicas) of all the indices specified in the request into native memory. After this process completes, a user will be able to start searching against their indices with no initial latency penalties. The warmup API is idempotent. If a segment's graphs are already loaded into memory, this operation will have no impact on them. It only loads graphs that are not currently in memory. 

### Usage
This command will perform warmup on index1, index2, and index3:
```
GET /_opendistro/_knn/warmup/index1,index2,index3?pretty
{
  "_shards" : {
    "total" : 6,
    "successful" : 6,
    "failed" : 0
  }
}
```
`total` indicates how many shards the warmup operation was performed on. `successful` indicates how many shards succeeded and `failed` indicates how many shards have failed.

The call will not return until the warmup operation is complete or the request times out. If the request times out, the operation will still be going on in the cluster. To monitor this, use the Elasticsearch `_tasks` API.

Following the completion of the operation, use the k-NN `_stats` API to see what has been loaded into the graph.

### Best practices
In order for the warmup API to function properly, a few best practices should be followed. First, no merge operations should be currently running on the indices that will be warmed up. The reason for this is that, during merge, new segments are created and old segments are (sometimes) deleted. The situation may arise where the warmup API loads graphs A and B into native memory, but then segment C is created from segments A and B being merged. The graphs for A and B will no longer be in memory and neither will the graph for C. Then, the initial penalty of loading graph C on the first queries will still be present.

Second, it should first be confirmed that all of the graphs of interest are able to fit into native memory before running warmup. If they all cannot fit into memory, then the cache will thrash.

## Scoring
During k-NN search, for each graph, NMSLIB will return up to `k` results. These results contain both the [document ID and the NMSLIB score](https://github.com/opendistro-for-elasticsearch/k-NN/blob/master/src/main/java/com/amazon/opendistroforelasticsearch/knn/index/KNNQueryResult.java#L21). 

The score NMSLIB assigns to a result is related to the space type that is selected. For example, for cosine similarity, NMSLIB will return [`1 - normScalarProduct`](https://github.com/nmslib/nmslib/blob/master/similarity_search/src/method/hnsw_distfunc_opt.cc#L372). For l2, the score returned will be the square of the euclidean distance.

From the k-NN and NMSLIB perspective, a lower score equates to a closer and better result. This is the opposite of how Elasticsearch scores results, where a greater score equates to a better result. In order to convert from the NMSLIB score to the Elasticsearch score, we perform [the following conversion](https://github.com/opendistro-for-elasticsearch/k-NN/blob/master/src/main/java/com/amazon/opendistroforelasticsearch/knn/index/KNNWeight.java#L113):
```
Elasticsearch score = 1/(1 + NMSLIB score)
```
This makes it so that the closer the result is to the query, the greater its Elasticsearch score will be.

## Contributions

We appreciate and encourage contributions from the community. If you experience a bug or have a feature request, please create an issue for it. If you decide to make a contribution, please fill out the Pull Request template with as much detail as possible. Also, when creating a title for your Pull Request, please do not include a prefix such as `Bug Fix:`. Instead, please use the corresponding tag to label the purpose of the Pull Request.

## REQUEST FOR COMMENT (RFC)

We'd like to get your comments! Please read the plugin RFC [document](https://github.com/opendistro-for-elasticsearch/k-NN/blob/development/RFC.md) and raise an issue to add your comments and questions.

## Credits and Acknowledgments

This project uses the Apache 2.0-licensed [Non-Metric Space Library](https://github.com/nmslib/nmslib/). Thank you to Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, David Novak and all those who have contributed to that project!

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
