## 2020-01-24 Version 1.4.0.0
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