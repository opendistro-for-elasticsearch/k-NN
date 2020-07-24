#KNN Performance Tuning


In this section we provide recommendations for performance tuning to improve indexing/search performance with the k-NN plugin.  On a high level k-NN works on following principles

* Graphs are created per (Lucene) segment
* Queries execute on segments sequentially inside the shard (same as any other Elasticsearch query). 
* Each graph in the segment returns ‘k’ neighbors and the size results with the highest score is returned by the coordinating node. Note that size can be greater or smaller than k.

To improve performance it is necessary to keep the number of segments under control. Ideally having 1 segment per shard will give the optimal performance with respect to search latency. We can achieve more parallelism by having more shards per index. We can control the number of segments either during indexing by asking Elasticsearch to slow down the segment creation by disabling the refresh interval or choosing larger refresh interval, increasing the flush threshold OR force-merging to 1 segment after all the indexing finishes and before searches.

##Indexing Performance Tuning

Following steps could help improve indexing performance especially when you plan to index large number of vectors at once. 

* Disable refresh interval  (Default = 1 sec)
 
 Disable refresh interval or set a long duration for refresh interval to avoid creating multiple smaller segments
  ```  
    PUT /<index_name>/_settings
        {
            "index" : {
                "refresh_interval" : "-1"
            }
        }
  ```
* Disable flush
 ```
    Increase the "index.translog.flush_threshold_size" to some bigger value lets say "10gb", default is 512MB
 ```
* No Replicas (No Elasticsearch replica shard)
 ```
    Having replication set to 0, will avoid duplicate construction of graphs in 
    both primary and replicas. When we enable replicas after the indexing, the 
    serialized graphs are directly copied.
 ```
    
* Increase number of indexing threads
  ```
    If the hardware we choose have multiple cores, we could allow multiple threads 
    in graph construction and there by speed up the indexing process. You could determine
    the number of threads to be alloted by using the following setting   
    https://github.com/opendistro-for-elasticsearch/k-NN#knnalgo_paramindex_thread_qty.
     
    Please keep an eye on CPU utilization and choose right number of threads. Since graph
    construction is costly, having multiple threads can put additional load on CPU. 
  ```
    
* Index all docs (Perform bulk indexing)

* Forcemerge 
  
 Forcemerge is a costly operation and could take a while depending on number of segments and size of the segments.
 To ensure force merge is completed, we could keep calling forcemerge with 5 minute interval till you get 200 response.
    
    curl -X POST "localhost:9200/myindex/_forcemerge?max_num_segments=1&pretty"
    
* Call refresh 

 Might not needed but to ensure the buffer is cleared and all segments are up. 
 ```
  POST /twitter/_refresh
```
* Add replicas (replica shards)

* We can now enable replicas to copy the serialized graphs

*  Enable refresh interval
 ```
      PUT /<index_name>/_settings
        {
            "index" : {
                "refresh_interval" : "1m"
            }
        }
 ```

Please refer following doc (https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-indexing-speed.html) for more details on improving indexing performance in general.

##Search Performance Tuning

### Warm up

The graphs are constructed during indexing, but they are loaded into memory during the first search. The way search works in Lucene is that each segment is searched sequentially (so, for k-NN, each segment returns up to k nearest neighbors of the query point) and the results are aggregated together and ranked based on the score of each result (higher score --> better result). 

Once a graph is loaded(graphs are loaded outside Elasticsearch JVM), we cache the graphs in memory. So the initial queries would be expensive in the order of few seconds and subsequent queries should be faster in the order of milliseconds(assuming knn circuit breaker is not hit).

In order to avoid this latency penalty during your first queries, a user should use the warmup API on the indices they want to search. The API looks like this:

GET /_opendistro/_knn/warmup/index1,index2,index3?pretty
{
  "_shards" : {
    "total" : 6,
    "successful" : 6,
    "failed" : 0
  }
}

The API loads all of the graphs for all of the shards (primaries and replicas) for the specified indices into the cache. Thus, there will be no penalty to load graphs during initial searches. *Note — * this API only loads the segments of the indices it sees into the cache. If a merge or refresh operation finishes after this API is ran or if new documents are added, this API will need to be re-ran to load those graphs into memory.

### Avoid reading stored fields

If the use case is to just read the nearest neighbors Ids and scores, then we could disable reading stored fields which could save some time retrieving the vectors from stored fields. 
To understand more about stored fields, 
please refer this [page.](https://discuss.elastic.co/t/what-does-it-mean-to-store-a-field/5893/5)
```
{
 "size": 5,
 "stored_fields": "_none_",
 "docvalue_fields": ["_id"],
 "query": {
   "knn": {
    "v": {
      "vector": [-0.16490704,-0.047262248,-0.078923926],
      "k": 50
     }       
   }
 }
}
```
##Improving Recall 

Recall could depend on multiple factors like number of dimensions, segments(searching over large number of small segments and aggregating the results leads better recall than searching over small number of large segments and aggregating results. The larger the graph the more chances of losing recall if sticking to smaller algorithm parameters. Choosing larger values for algo params should help solve this issue), number of vectors, etc.  Recall can be configured by adjusting the algorithm parameters of hnsw algorithm exposed through index settings. Algorithm params that control the recall are *m, ef_construction, ef_search*. For more details on influence of algorithm parameters on the indexing, search recall, please refer this  doc (https://github.com/nmslib/hnswlib/blob/master/ALGO_PARAMS.md).  Increasing these values could help recall(better search results) but at the cost of higher memory utilization and increased indexing time. Our default values work on a broader set of use cases from our experiments but we encourage users to run their own experiments on their data sets and choose the appropriate values. You could refer to these settings in this section (https://github.com/opendistro-for-elasticsearch/k-NN#index-level-settings). We will add details on our experiments shortly here.

Memory Estimation

AWS Elasticsearch Service clusters allocate 50% of available RAM in the Instance capped around 32GB (because of JVM GC performance limit). Graphs part of k-NN are loaded outside the Elasticsearch process JVM. We have circuit breakers to limit graph usage to 50% of the left over RAM space for the graphs.

* Memory required for graphs =   1.1 *((4* dimensions) + (8 * M)) *Bytes/vector*
    * (4 bytes/float * dimension float/vector)
    * (8 * M) = 4 bytes/edge * 2 levels/node *  M edge/level
        * Note — as an estimation, each node will have membership in roughly 2 layers, and, on each layer, it will have M edges
    * 1.1 = an extra 10% buffer for other meta data in the data structure
* Example:- Let us assume
    * 1 Million vectors 
    * 256 Dimensions (2^8)
    * M = 16 (default setting of HNSW)
        * Memory required for !M vectors = 1.1*(4*256 + 8*16) *1M Bytes =~ 1.26GB 

##Monitoring 

The KNN Stats API provides information about the current status of the KNN Plugin. The plugin keeps track of both cluster level and node level stats. Cluster level stats have a single value for the entire cluster. Node level stats have a single value for each node in the cluster. A user can filter their query by nodeID and statName in the following way:
 ```
  GET /_opendistro/_knn/nodeId1,nodeId2/stats/statName1,statName2
 ```

Detailed breakdown of stats api metrics here https://github.com/opendistro-for-elasticsearch/k-NN#cluster-stats
