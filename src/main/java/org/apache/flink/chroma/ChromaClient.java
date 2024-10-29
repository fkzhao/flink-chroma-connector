package org.apache.flink.chroma;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.chroma.conf.AuthType;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ChromaClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ChromaClient.class);
    private static final String API_CHECK_HEALTH = "/api/v1/heartbeat";
    private static final String API_DATABASES = "/api/v1/databases";
    private static final String API_COLLECTIONS = "/api/v1/collections";



    CloseableHttpClient httpClient;
    private final transient ScheduledExecutorService scheduledExecutorService;

    private final String url;
    private final int intervalTime;
    private boolean isHealth;

    public ChromaClient(String url, AuthType authType, String authIdentify) {
        this.url = url;
        if (authType != AuthType.AuthTypeNone && authIdentify == null) {
            throw new RuntimeException("Chroma DB auth identify is null.");
        }
        this.intervalTime = 60000;
        RequestConfig requestConfigStream = RequestConfig.custom()
                .setConnectTimeout(60 * 1000)
                .setConnectionRequestTimeout(60 * 1000)
                .build();
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
        this.httpClient = httpClientBuilder.build();
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("chroma-db-status-check"));
        initializeLoad();
    }

    void initializeLoad() {
        this.checkHealth();
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkHealth, 200, intervalTime, TimeUnit.MILLISECONDS);
    }


    public void checkHealth() {
        synchronized (this) {
            logger.info("Chroma DB health check started.");
            HttpGet httpGet = new HttpGet(url + API_CHECK_HEALTH);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                isHealth = response.getStatusLine().getStatusCode() == 200;
                logger.info("Chroma DB health status is: {}", isHealth);
            } catch (IOException e) {
                isHealth = false;
                logger.error("Chroma DB health check failed.", e);
            }
        }
    }

    public boolean checkTenantAndDatabase(String tenant, String database) {
        HttpGet request = new HttpGet(url + API_DATABASES + "/" + database + "?tenant=" + tenant);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            logger.info("Response Status: {}", response.getStatusLine().getStatusCode());
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("Response Body: {}", responseBody);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (IOException e) {
            isHealth = false;
        }
        return false;
    }

    public boolean checkCollection(String tenant, String database, String collection) {
        HttpGet request = new HttpGet(url + API_COLLECTIONS + "?tenant=" + tenant + "&database=" + database);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            JSONArray jsonArray = JSONArray.parseArray(responseBody);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject collectionInfo = jsonArray.getJSONObject(i);
                String collectionName = collectionInfo.getString("name");
                if (collectionName.equals(collection)) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
           logger.error(e.getMessage(), e);
        }
        return false;
    }

    public boolean createCollection(String tenant, String database, String collection) {
        HttpPost request = new HttpPost(url + API_COLLECTIONS + "?tenant=" + tenant + "&database=" + database);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", collection);
        request.setEntity(new StringEntity(jsonObject.toJSONString(), StandardCharsets.UTF_8));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("createCollection response body: {}", responseBody);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean upsertDocument(String collection, String document) {
        HttpPost request = new HttpPost(url + API_COLLECTIONS + "/" + collection + "/upsert");
        request.setEntity(new StringEntity(document, StandardCharsets.UTF_8));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("upsertDocument response body: {}", responseBody);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public boolean isHealth() {
        return isHealth;
    }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String url;
        private AuthType authType;
        private String authIdentify;
        public Builder url(String url) {
            this.url = url;
            return this;
        }
        public Builder authType(AuthType authType) {
            this.authType = authType;
            return this;
        }
        public Builder authIdentify(String authIdentify) {
            this.authIdentify = authIdentify;
            return this;
        }

        public ChromaClient build() {
            return new ChromaClient(url, authType, authIdentify);
        }
    }

}
