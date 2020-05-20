## 2020-03-24 Version 1.6.0.0 (Current)
### Features
* Feature [#76](https://github.com/opendistro-for-elasticsearch/k-NN/pull/72): Elasticsearch 7.6.1 compatibility (issue [#71](https://github.com/opendistro-for-elasticsearch/k-NN/issues/71))
* Feature [#73](https://github.com/opendistro-for-elasticsearch/k-NN/pull/73): Add Github Actions so that changes are automatically tested and artifacts are uploaded to S3 (issue [#74](https://github.com/opendistro-for-elasticsearch/k-NN/issues/74))

### Enhancements
* Enhancement [#61](https://github.com/opendistro-for-elasticsearch/k-NN/pull/61): Convert integration tests from ESIntegTestCase to ESRestTestCase, so that they can be run on a remote cluster (issue [#60](https://github.com/opendistro-for-elasticsearch/k-NN/issues/60))
* Enhancement [#54](https://github.com/opendistro-for-elasticsearch/k-NN/pull/54): Add check in gradle build for license headers (issue [#7](https://github.com/opendistro-for-elasticsearch/k-NN/issues/7))
* Enhancement [#52](https://github.com/opendistro-for-elasticsearch/k-NN/pull/52): Lazily load efSearch parameter (issue [#51](https://github.com/opendistro-for-elasticsearch/k-NN/issues/51))

### Bug Fixes
* Bugfix [#66](https://github.com/opendistro-for-elasticsearch/k-NN/pull/66): Flaky failure in KNN80HnswIndexTests testFooter (issue [#65](https://github.com/opendistro-for-elasticsearch/k-NN/issues/65))
* Bugfix [#63](https://github.com/opendistro-for-elasticsearch/k-NN/pull/63): Circuit Breaker fails to turn off (issue [#62](https://github.com/opendistro-for-elasticsearch/k-NN/issues/62))
* Bugfix [#59](https://github.com/opendistro-for-elasticsearch/k-NN/pull/59): Gradle build failure on Mac due to library error (issue [#58](https://github.com/opendistro-for-elasticsearch/k-NN/issues/58))
* Bugfix [#53](https://github.com/opendistro-for-elasticsearch/k-NN/pull/53): AccessControlException when HNSW library is loaded (issue [#49](https://github.com/opendistro-for-elasticsearch/k-NN/issues/49))
* Bugfix [#47](https://github.com/opendistro-for-elasticsearch/k-NN/pull/47): Stats API failure in Transport Layer (issue [#45](https://github.com/opendistro-for-elasticsearch/k-NN/issues/45))
