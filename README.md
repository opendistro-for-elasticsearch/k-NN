[![Testing Workflow](https://github.com/opendistro-for-elasticsearch/k-NN/workflows/Testing%20Workflow/badge.svg)](https://github.com/opendistro-for-elasticsearch/k-NN/actions)
[![codecov](https://codecov.io/gh/opendistro-for-elasticsearch/k-NN/branch/master/graph/badge.svg)](https://codecov.io/gh/opendistro-for-elasticsearch/k-NN)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://opendistro.github.io/for-elasticsearch-docs/docs/knn/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/k-NN/)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

# Open Distro for Elasticsearch KNN

Open Distro for Elasticsearch enables you to run nearest neighbor search on billions of documents across thousands of dimensions with the same ease as running any regular Elasticsearch query. You can use aggregations and filter clauses to further refine your similarity search operations. K-NN similarity search powers use cases such as product recommendations, fraud detection, image and video search, related document search, and more.

## Documentation

The README provides information for development of the k-NN plugin. To learn more about plugin usage, please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/docs/knn). Do not hesitate to [create an issue](https://github.com/opendistro-for-elasticsearch/k-NN/issues/new) if something is missing from the documentation!

## Setup

1. Check out the package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.
3. To build from the command line, set `JAVA_HOME` to point to a JDK 14 before running `./gradlew build`.

## Build

The package uses the [Gradle](https://docs.gradle.org/6.6.1/userguide/userguide.html) build system.

1. Checkout this package from version control.
2. To build from command line set `JAVA_HOME` to point to a JDK >=14
3. Run `./gradlew build`

## JNI Library

The plugin relies on a JNI library to perform approximate k-NN search. For plugin installations from archive(.zip), it is necessary to ensure ```.so``` file for Linux and ```.jnilib``` file for Mac OS are present in the Java library path. This can be possible by copying .so/.jnilib to either $ES_HOME or by adding manually ```-Djava.library.path=<path_to_lib_files>``` in ```jvm.options``` file

To build the JNI Library, follow these steps:

```
cd jni
cmake .
make
```

The library will be placed in the `jni/release` directory.

To build an RPM or DEB of the JNI library, follow these steps:

```
cd jni
cmake .
make package
```

The artifacts will be placed in the `jni/packages` directory.

## JNI Library Artifacts

We build and distribute binary library artifacts with Opendistro for Elasticsearch. We build the library binary, RPM and DEB in [this GitHub action](https://github.com/opendistro-for-elasticsearch/k-NN/blob/main/.github/workflows/CD.yml). We use Centos 7 with g++ 4.8.5 to build the DEB, RPM and ZIP. Additionally, in order to provide as much general compatibility as possible, we compile the library without optimized instruction sets enabled. For users that want to get the most out of the library, they should follow [this section](#jni-library) and build the library from source in their production environment, so that if their environment has optimized instruction sets, they take advantage of them.

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

Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
