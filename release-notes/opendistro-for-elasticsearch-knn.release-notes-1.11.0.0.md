## Version 1.11.0.0 Release Notes

Compatible with Elasticsearch 7.9.1
### Features

* Pre filter support through custom scoring ([#196](https://github.com/opendistro-for-elasticsearch/k-NN/pull/196))

### Enhancements

* Add existsQuery method implementation to KNNVectorFieldType ([#228](https://github.com/opendistro-for-elasticsearch/k-NN/pull/228))
* Change "space" parameter to "space_type" for custom scoring ([#232](https://github.com/opendistro-for-elasticsearch/k-NN/pull/232))
* change space -> space_type ([#234](https://github.com/opendistro-for-elasticsearch/k-NN/pull/234))
* Add stats for custom scoring feature ([#233](https://github.com/opendistro-for-elasticsearch/k-NN/pull/233))

### Bug Fixes

* KNN score fix for non knn documents ([#231](https://github.com/opendistro-for-elasticsearch/k-NN/pull/231))
* Fix script statistics flaky test case ([#235](https://github.com/opendistro-for-elasticsearch/k-NN/pull/235))
* Refactor KNNVectorFieldMapper ([#240](https://github.com/opendistro-for-elasticsearch/k-NN/pull/240))
* Fix PostingsFormat in KNN Codec ([#236](https://github.com/opendistro-for-elasticsearch/k-NN/pull/236))

