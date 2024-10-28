package org.apache.flink.chroma.utils;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.util.concurrent.TimeUnit;

public class HttpUtil {
    private RequestConfig requestConfigStream =
            RequestConfig.custom()
                    .setConnectTimeout(60 * 1000)
                    .setConnectionRequestTimeout(60 * 1000)
                    .build();

    private final HttpClientBuilder httpClientBuilder =
            HttpClients.custom()
                    .setRedirectStrategy(
                            new DefaultRedirectStrategy() {
                                @Override
                                protected boolean isRedirectable(String method) {
                                    return true;
                                }
                            })
                    .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                    .evictExpiredConnections()
                    .evictIdleConnections(60, TimeUnit.SECONDS)
                    .setDefaultRequestConfig(requestConfigStream);

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }

    private RequestConfig requestConfig =
            RequestConfig.custom()
                    .setConnectTimeout(60 * 1000)
                    .setConnectionRequestTimeout(60 * 1000)
                    // default checkpoint timeout is 10min
                    .setSocketTimeout(9 * 60 * 1000)
                    .build();

    public HttpClientBuilder getHttpClientBuilderForBatch() {
        return HttpClients.custom()
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        })
                .setDefaultRequestConfig(requestConfig);
    }

    public HttpClientBuilder getHttpClientBuilderForCopyBatch() {
        return HttpClients.custom()
                .disableRedirectHandling()
                .setDefaultRequestConfig(requestConfig);
    }
}
