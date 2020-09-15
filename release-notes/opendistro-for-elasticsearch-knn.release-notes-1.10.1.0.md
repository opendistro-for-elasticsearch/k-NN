## Version 1.10.1.0 Release Notes

Compatible with Elasticsearch 7.9.1
### Features

* Add Warmup API to load indices graphs into memory ([#162](https://github.com/opendistro-for-elasticsearch/k-NN/pull/162))

### Enhancements

* Upgrade nmslib to v2.0.6 ([#160](https://github.com/opendistro-for-elasticsearch/k-NN/pull/160))

### Bug Fixes

* Update guava version to 29.0 ([#182](https://github.com/opendistro-for-elasticsearch/k-NN/pull/182))
* Add default index settings when parsing index ([#205](https://github.com/opendistro-for-elasticsearch/k-NN/pull/205))
* NPE in force merge when non knn doc gets updated to knn doc across segments ([#212](https://github.com/opendistro-for-elasticsearch/k-NN/pull/212))
* Fix casting issue with cache expiration ([#215](https://github.com/opendistro-for-elasticsearch/k-NN/pull/215))

### Infrastructure

* Reset state for uTs so tests run independently ([#159](https://github.com/opendistro-for-elasticsearch/k-NN/pull/159))
* Pass -march=x86-64 to build JNI library ([#164](https://github.com/opendistro-for-elasticsearch/k-NN/pull/164))
* Fix versioning for lib artifacts ([#166](https://github.com/opendistro-for-elasticsearch/k-NN/pull/166))
* Add release notes automation ([#168](https://github.com/opendistro-for-elasticsearch/k-NN/pull/168))
* Add Github action to build library artifacts ([#170](https://github.com/opendistro-for-elasticsearch/k-NN/pull/170))
* Flaky rest test case fix ([#183](https://github.com/opendistro-for-elasticsearch/k-NN/pull/183))
* Add code coverage widget and badges ([#191](https://github.com/opendistro-for-elasticsearch/k-NN/pull/191))
* Add Codecov configuration to set a coverage threshold to pass the check on a commit ([#192](https://github.com/opendistro-for-elasticsearch/k-NN/pull/192))
* Add AWS CLI in order to ship library artifacts from container ([#194](https://github.com/opendistro-for-elasticsearch/k-NN/pull/194))
* Remove sudo from "./aws install" in library build action ([#202](https://github.com/opendistro-for-elasticsearch/k-NN/pull/202))
* Fix download link in package description ([#214](https://github.com/opendistro-for-elasticsearch/k-NN/pull/214))

### Documentation

* Performance tuning/Recommendations ([#177](https://github.com/opendistro-for-elasticsearch/k-NN/pull/177))
* Fix cluster setting example in README.md ([#186](https://github.com/opendistro-for-elasticsearch/k-NN/pull/186))
* Add scoring documentation ([#193](https://github.com/opendistro-for-elasticsearch/k-NN/pull/193))
* Add 1.10.0.0 release notes ([#201](https://github.com/opendistro-for-elasticsearch/k-NN/pull/201))

### Maintenance

* ODFE 1.10 support for k-NN plugin ([#199](https://github.com/opendistro-for-elasticsearch/k-NN/pull/199))
* Upgrade Elasticsearch to 7.9.1 and ODFE to 1.10.1 ([#217](https://github.com/opendistro-for-elasticsearch/k-NN/pull/217))

### Refactoring

* Update default variable settings name ([#209](https://github.com/opendistro-for-elasticsearch/k-NN/pull/209))

