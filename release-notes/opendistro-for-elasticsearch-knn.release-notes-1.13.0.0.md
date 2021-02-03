## Version 1.13.0.0 Release Notes

Compatible with Elasticsearch 7.10.2

### Features

* Support k-NN similarity functions in painless scripting ([#281](https://github.com/opendistro-for-elasticsearch/k-NN/pull/281))
* Add support for L1 distance in AKNN, custom scoring and painless scripting ([#310](https://github.com/opendistro-for-elasticsearch/k-NN/pull/310))

### Enhancements

* Upgrade nmslib to 2.0.11 ([#302](https://github.com/opendistro-for-elasticsearch/k-NN/pull/302))
* Upgrade commons-beanutils ([#297](https://github.com/opendistro-for-elasticsearch/k-NN/pull/297))

### Bug Fixes

* Fix find_path bug in CMakeLists ([#280](https://github.com/opendistro-for-elasticsearch/k-NN/pull/280))
* Add builder constructor that takes algo params ([#289](https://github.com/opendistro-for-elasticsearch/k-NN/pull/289))

### Infrastructure

* Add arm64 support and correct the naming convention to the new standards ([#299](https://github.com/opendistro-for-elasticsearch/k-NN/pull/299))
* Run KNN integ tests with security plugin enabled ([#304](https://github.com/opendistro-for-elasticsearch/k-NN/pull/304))
* Update artifact naming ([#309](https://github.com/opendistro-for-elasticsearch/k-NN/pull/309))
* Change CD workflow to use new staging bucket for artifacts ([#301](https://github.com/opendistro-for-elasticsearch/k-NN/pull/301))

### Documentation

* Add copyright header ([#307](https://github.com/opendistro-for-elasticsearch/k-NN/pull/307))

### Maintenance

* Upgrade odfe version to 1.13.0 ([#312](https://github.com/opendistro-for-elasticsearch/k-NN/pull/312))

