package org.apache.flink.chroma.utils;

import org.apache.flink.chroma.conf.AuthType;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpUtil {

    private static volatile CloseableHttpClient httpClient;
    private static final RequestConfig requestConfigStream = RequestConfig.custom()
            .setConnectTimeout(60 * 1000)
            .setConnectionRequestTimeout(60 * 1000)
            .build();

    public static CloseableHttpClient getChromaHttpClient(AuthType authType, String authIdentify) {
        if (httpClient == null) {
            synchronized (HttpUtil.class) {
                if (httpClient == null) {
                    List<BasicHeader> headers = new ArrayList<>();
                    headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));
                    if (authType == AuthType.AuthTypeBasic) {
                        String encodedAuth = Base64.getEncoder().encodeToString(authIdentify.getBytes(StandardCharsets.UTF_8));
                        headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth));
                    } else if (authType == AuthType.AuthTypeBearer) {
                        headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authIdentify));
                    } else if (authType == AuthType.AuthTypeToken) {
                        headers.add(new BasicHeader("X_CHROMA_TOKEN", authIdentify));
                    }
                    HttpClientBuilder httpClientBuilder =
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
                                    .setDefaultRequestConfig(requestConfigStream)
                                    .setDefaultHeaders(headers);
                    httpClient = httpClientBuilder.build();
                }
            }
        }
        return httpClient;
    }
}
