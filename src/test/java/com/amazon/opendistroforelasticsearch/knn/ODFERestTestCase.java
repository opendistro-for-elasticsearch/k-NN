/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.knn;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

/**
 * ODFE integration test base class to support both security disabled and enabled ODFE cluster.
 */
public abstract class ODFERestTestCase extends ESRestTestCase {

    protected boolean isHttps() {
        boolean isHttps = Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
        if (isHttps) {
            // currently only external cluster is supported for security enabled testing
            if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
                throw new RuntimeException("cluster url should be provided for security enabled testing");
            }
        }

        return isHttps;
    }

    @Override
    protected String getProtocol() {
        return isHttps() ? "https" : "http";
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }

        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    protected static void configureHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            String userName = Optional
                    .ofNullable(System.getProperty("user"))
                    .orElseThrow(() -> new RuntimeException("user name is missing"));
            String password = Optional
                    .ofNullable(System.getProperty("password"))
                    .orElseThrow(() -> new RuntimeException("password is missing"));
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            try {
                return httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        // disable the certificate since our testing cluster just uses the default security configuration
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue
                .parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    /**
     * wipeAllIndices won't work since it cannot delete security index. Use wipeAllODFEIndices instead.
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @After
    protected void wipeAllODFEIndices() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (
                XContentParser parser = xContentType
                        .xContent()
                        .createParser(
                                NamedXContentRegistry.EMPTY,
                                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                response.getEntity().getContent()
                        )
        ) {
            XContentParser.Token token = parser.nextToken();
            List<Map<String, Object>> parserList = null;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            for (Map<String, Object> index : parserList) {
                String indexName = (String) index.get("index");
                if (indexName != null && !".opendistro_security".equals(indexName)) {
                    client().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }
}

