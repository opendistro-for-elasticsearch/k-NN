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

## Java Native library usage
For plugin installations from archive(.zip), it is necessary to ensure ```.so``` file for linux OS and ```.jnilib``` file for Mac OS are present in the java library path. This can be possible by copying .so/.jnilib to either $ES_HOME or by adding manually ```-Djava.library.path=<path_to_lib_files>``` in ```jvm.options``` file

## Docker
We put a docker-compose.yml file for you and which equipped with a custom Dockerfile
that build and install `opendistro-knn` elasticsearch plugin.

Please issue the following command to start your elasticsearch container:

```shell script
docker-compose up -d --build
```
This will start the elasticsearch on 9200 port. You may access it at https://localhost:9200 

Default username and password is:

  - Username: `admin`
  - Password: `admin`

**Get a list of installed plugins in elasticsearch:**
```shell script
curl -XGET https://localhost:9200/_cat/plugins?v -u admin:admin --insecure
```

**Create an index:**
Please open your terminal then paste the run the following curl command in console:
```
curl -XPUT "https://localhost:9200/demo" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "codec": "KNNCodec"
    }
  },
  "mappings": {
    "properties": {
      "my_vector": {
        "type": "knn_vector"
      },
      "price": {
        "type": "integer"
      },
      "name": {
        "type": "text"
      }
    }
  }
}
'
```
Click on play button and your index is created.

**Push some data:**
```shell script
curl -X PUT "https://localhost:9200/demo/_doc/1" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "my_vector": [1.5, 2.5],
  "price": 10,
  "name": "Nurul"
}
'

curl -X PUT "https://localhost:9200/demo/_doc/2" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "my_vector": [2.5, 3.5],
  "price": 12,
  "name": "Ferdous"
}
'

curl -X PUT "https://localhost:9200/demo/_doc/3" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "my_vector": [3.5, 4.5],
  "price": 15,
  "name": "Dynamic"
}
'

curl -X PUT "https://localhost:9200/demo/_doc/4" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "my_vector": [5.5, 6.5],
  "price": 17,
  "name": "Guy"
}'

curl -X PUT "https://localhost:9200/demo/_doc/5" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{
  "my_vector": [6.5, 7.5],
  "price": 18,
  "name": "Nurul Ferdous"
}
'
```

**Search for a KNN query:**
```shell script
curl -X POST "https://localhost:9200/demo/_search" -u admin:admin --insecure -H 'Content-Type: application/json' -d'
{"size" : 10,
 "query": {
  "knn": {
   "my_vector": {
     "vector": [3, 4],
     "k": 3
   }
  }
 }
}
'
```

**If everything goes well you would see a response like this:**
```json
{
  "took": 29,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    },
    "max_score": 1.4142135,
    "hits": [
      {
        "_index": "demo",
        "_type": "_doc",
        "_id": "2",
        "_score": 1.4142135,
        "_source": {
          "my_vector": [
            2.5,
            3.5
          ],
          "price": 12,
          "name": "Ferdous"
        }
      },
      {
        "_index": "demo",
        "_type": "_doc",
        "_id": "3",
        "_score": 1.4142135,
        "_source": {
          "my_vector": [
            3.5,
            4.5
          ],
          "price": 15,
          "name": "Dynamic"
        }
      },
      {
        "_index": "demo",
        "_type": "_doc",
        "_id": "1",
        "_score": 0.47140455,
        "_source": {
          "my_vector": [
            1.5,
            2.5
          ],
          "price": 10,
          "name": "Nurul"
        }
      }
    ]
  }
}
```

If you want to use a prebuilt container here is a public one: https://hub.docker.com/r/ferdous/odfes_elasticsearch_knn

Enjoy!

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE.txt) file for our project's licensing. We will ask you to confirm the licensing of your contribution.


## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.