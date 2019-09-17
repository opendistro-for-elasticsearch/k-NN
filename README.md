# Open Distro for Elasticsearch KNN
========================================

Open Distro for Elasticsearch enables you to run nearest neighbor search on billions of documents across thousands of dimensions with the same ease as running any regular Elasticsearch query. Use aggregations and filter clauses to further refine your similarity search operations. K-NN similarity search power use cases such as product recommendations, fraud detection, image and video search, related document search, and more.

## Documentation

Please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/).

## Setup

1. Check out this package from version control.
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
You could  create a KNN type index by specifying codec as *KNNCodec* and mark the particular field as of type ‘*knn_vector’*.
The following code creates a KNN index with fields my_vector1, my_vector2, my_vector3 as knn types. The knn type field accepts array of float.

```
PUT /myindex
{
  "settings" : {
    "index": {
      "codec": "KNNCodec"
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

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
