## Version 1.7.0 (Version compatible with elasticsearch 7.6.1)
### Features
* Feature [#90](https://github.com/opendistro-for-elasticsearch/k-NN/pull/90): Support cosine similarity (issue [#28](https://github.com/opendistro-for-elasticsearch/k-NN/issues/28)). ```Note``` this feature is experimental

### Enhancements
* Enhancement [#89](https://github.com/opendistro-for-elasticsearch/k-NN/pull/89): Add stats to track the number of requests and errors for KNN query and index operations. (issue [#88](https://github.com/opendistro-for-elasticsearch/k-NN/issues/88))
* Enhancement [#92](https://github.com/opendistro-for-elasticsearch/k-NN/pull/92): Switched the default value of the circuit breaker from 60% to 50%. (issue [#82](https://github.com/opendistro-for-elasticsearch/k-NN/issues/82))
* Enhancement [#73](https://github.com/opendistro-for-elasticsearch/k-NN/pull/73): Create Github action that automatically runs integration tests against docker image whenever code is checked into master or opendistro branch. (issue [#74](https://github.com/opendistro-for-elasticsearch/k-NN/issues/74))

### Bug Fixes
* Bugfix [#100](https://github.com/opendistro-for-elasticsearch/k-NN/pull/100): Added validation in VectorFieldMapper to check for vector values of NaN and throwing an Exception if so. (issue [#99](https://github.com/opendistro-for-elasticsearch/k-NN/issues/99))
* Bugfix [#78](https://github.com/opendistro-for-elasticsearch/k-NN/pull/78): Fix debugging integration tests (issue [#77](https://github.com/opendistro-for-elasticsearch/k-NN/issues/77))
