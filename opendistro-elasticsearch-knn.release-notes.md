# Release Notes
## 2020-01-24 Version 1.4.0.0 (Current)
### Features
#### Elasticsearch Compatibility
* Feature [#11 ](https://github.com/opendistro-for-elasticsearch/k-NN/issues/11): Elasticsearch 7.4.2 compatibility

#### Documentation
* Feature ( [#40 ](https://github.com/opendistro-for-elasticsearch/k-NN/issues/40 ), [#37 ](https://github.com/opendistro-for-elasticsearch/k-NN/issues/37)). Documentation on knn index creation, settings and stats 

### Enhancements
* KNN Codec Backward Compatibility support  [#20  ](https://github.com/opendistro-for-elasticsearch/k-NN/issues/20)

### Bug Fixes
* Avoid recreating space for each query [#29 ]
* Fix a leak where FileWatchers are added but never removed [#36 ]
* JNI clean up and race conditions [#25 ]
*  native memory leak in saveIndex and JVM leak [#14 ] [#15 ]

### Note
For configuring native library for plugin installations from Archive, please refer to [ReadME](https://github.com/opendistro-for-elasticsearch/k-NN/blob/development/README.md#java-native-library-usage)

## 2019-12-03 Version 1.3.0.0
### Notable changes

* opendistro1.3 for KNN for ES 7.3.2
* Performance improvement and bug fixes
** JNI Memory leak fixes
** Cache time out fix
** Doc values memory fix
** Other minor bugs
** Updated ReadMe
* fixed memory leak in saveIndex
* fixed issue jvm heap leak in JNI

Note:- For configuring native library for plugin installations from Archive, please refer to [ReadME](https://github.com/opendistro-for-elasticsearch/k-NN/blob/development/README.md#java-native-library-usage)

## 2019-09-17 Version 1.2.0.0-alpha.1
### New Features
  * Adds support for Elasticsearch 7.2.0 - [Commit #](https://github.com/opendistro-for-elasticsearch/k-NN/commit/15ae8c7b3a4ab88e2be974af107161b10d0204bb)

### Bug fixes
  * Performance improvement and bug fixes - [PR 2](https://github.com/opendistro-for-elasticsearch/k-NN/pull/2)

Note:- For configuring native library for plugin installations from Archive, please refer to [ReadME](https://github.com/opendistro-for-elasticsearch/k-NN/blob/development/README.md#java-native-library-usage)
