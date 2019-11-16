FROM amazon/opendistro-for-elasticsearch:1.2.0
RUN yum -y update
RUN yum -y groupinstall "Development Tools"
RUN yum -y install glibc.x86_64 cmake

RUN curl -s https://download.java.net/java/GA/jdk12.0.2/e482c34c86bd4bf8b56c0b35558996b9/10/GPL/openjdk-12.0.2_linux-x64_bin.tar.gz | tar -C /opt -zxf -
ENV JAVA_HOME /opt/jdk-12.0.2

ENV PATH $JAVA_HOME/bin:$PATH

RUN set -ex; \
    \
    git clone https://github.com/nmslib/nmslib.git /usr/share/elasticsearch/nmslib; \
    \
    cd /usr/share/elasticsearch/nmslib \
    \
    && git checkout tags/v1.7.3.6 \
    \
    && cd similarity_search \
    \
    && cmake . \
    \
    && make install; \
    \
    git clone https://github.com/opendistro-for-elasticsearch/k-NN.git /usr/share/elasticsearch/k-NN; \
    \
    cd /usr/share/elasticsearch/k-NN \
    \
    && git checkout V1.2.0.0-alpha.1 \
    \
    && mkdir /tmp/jni \
    \
    && cp jni/src/v1736/* /tmp/jni \
    \
    && cd /tmp/jni \
    \
    && g++ -fPIC -I/opt/jdk-12.0.2/include -I/opt/jdk-12.0.2/include/linux -I/usr/share/elasticsearch/nmslib/similarity_search/include -std=c++11 -shared -o libKNNIndexV1_7_3_6.so org_elasticsearch_index_knn_v1736_KNNIndex.cpp -lNonMetricSpaceLib -L/usr/share/elasticsearch/nmslib/similarity_search/release \
    \
    && mv /tmp/jni/libKNNIndexV1_7_3_6.so /usr/share/elasticsearch/k-NN/buildSrc \
    \
    && cd /usr/share/elasticsearch/k-NN \
    \
    && ./gradlew build -x :integTestRunner -x :dumpCoverage -x :jacocoTestReport -x test \
    \
    && cp /usr/share/elasticsearch/k-NN/buildSrc/libKNNIndexV1_7_3_6.so /usr/lib

RUN cd /usr/share/elasticsearch

RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch https://github.com/opendistro-for-elasticsearch/k-NN/releases/download/V1.2.0.0-alpha.1/opendistro-knn-1.2.0.0.zip