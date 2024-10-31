package org.apache.flink.chroma;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.chroma.conf.AuthType;
import org.apache.flink.chroma.utils.HttpUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChromaClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ChromaClient.class);
    private static final String API_CHECK_HEALTH = "/api/v1/heartbeat";
    private static final String API_DATABASES = "/api/v1/databases";
    private static final String API_COLLECTIONS = "/api/v1/collections";

    private final Map<String, ChromaCollection> collectionsCache = new HashMap<>();

    private final String url;
    private final String tenant;
    private final String database;
    private final AuthType authType;
    private final String authIdentify;

    public ChromaClient(String url, AuthType authType, String authIdentify, String tenant, String database) {

        this.url = url;
        this.tenant = tenant;
        this.database = database;
        this.authType = authType;
        this.authIdentify = authIdentify;
        if (authType != AuthType.AuthTypeNone && authIdentify == null) {
            throw new RuntimeException("Chroma DB auth identify is null.");
        }
        initializeCheck();
    }

    void initializeCheck() {
        if (!checkTenantAndDatabase()) {
            throw new RuntimeException("Chroma tenant: " + tenant + " database: " + database + " is not exist.");
        }
    }


    public void checkHealth() {
        synchronized (this) {
            logger.info("Chroma DB health check started.");
            HttpGet httpGet = new HttpGet(url + API_CHECK_HEALTH);
            try (CloseableHttpResponse response = getHttpClient().execute(httpGet)) {
                logger.info("Chroma DB health status is: true");
            } catch (IOException e) {
                logger.error("Chroma DB health check failed.", e);
            }
        }
    }

    private boolean checkTenantAndDatabase() {
        HttpGet request = new HttpGet(url + API_DATABASES + "/" + database + "?tenant=" + tenant);
        try (CloseableHttpResponse response = getHttpClient().execute(request)) {
            logger.info("Response Status: {}", response.getStatusLine().getStatusCode());
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("Response Body: {}", responseBody);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    public ChromaCollection getCollection(String collection) {
        //
        ChromaCollection chromaCollection = collectionsCache.get(collection);
        if (chromaCollection != null) {
            return chromaCollection;
        }

        HttpGet request = new HttpGet(url + API_COLLECTIONS + "?tenant=" + tenant + "&database=" + database);
        try (CloseableHttpResponse response = getHttpClient().execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            List<ChromaCollection> collections = JSONArray.parseArray(responseBody, ChromaCollection.class);
            for (ChromaCollection c : collections) {
                c.setAuthType(authType);
                c.setAuthIdentify(authIdentify);
                c.setUrl(url);
                collectionsCache.put(c.getName(), c);
            }

            //
            chromaCollection = collectionsCache.get(collection);
            if (chromaCollection != null) {
                return chromaCollection;
            }

            //
            createCollection(collection);

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public void createCollection(String collection) {
        HttpPost request = new HttpPost(url + API_COLLECTIONS + "?tenant=" + tenant + "&database=" + database);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", collection);
        request.setEntity(new StringEntity(jsonObject.toJSONString(), StandardCharsets.UTF_8));
        try (CloseableHttpResponse response = getHttpClient().execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("createCollection response body: {}", responseBody);
            response.getStatusLine().getStatusCode();
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }
    }

    private CloseableHttpClient getHttpClient() {
        return HttpUtil.getChromaHttpClient(authType, authIdentify);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private AuthType authType;
        private String authIdentify;
        private String tenant;
        private String database;

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

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public ChromaClient build() {
            return new ChromaClient(url, authType, authIdentify, tenant, database);
        }
    }

}
