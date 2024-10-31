package org.apache.flink.chroma;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.flink.chroma.conf.AuthType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.chroma.utils.HttpUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ChromaCollection implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(ChromaClient.class);
    private static final String API_COLLECTIONS = "/api/v1/collections";

    @JSONField(serialize = false)
    private String url;
    @JSONField(serialize = false)
    private AuthType authType;
    @JSONField(serialize = false)
    private String authIdentify;

    @JSONField(name = "id")
    private String id;
    @JSONField(name = "name")
    private String name;
    @JSONField(name = "tenant")
    private String tenant;
    @JSONField(name = "database")
    private String database;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public AuthType getAuthType() {
        return authType;
    }

    public void setAuthType(AuthType authType) {
        this.authType = authType;
    }

    public String getAuthIdentify() {
        return authIdentify;
    }

    public void setAuthIdentify(String authIdentify) {
        this.authIdentify = authIdentify;
    }


    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean upsertDocument(String document) {
        Preconditions.checkNotNull(url);
        Preconditions.checkNotNull(authType);
        if (authType != AuthType.AuthTypeNone) {
            Preconditions.checkNotNull(authIdentify);
        }
        Preconditions.checkNotNull(document);
        CloseableHttpClient httpClient = HttpUtil.getChromaHttpClient(authType, authIdentify);
        HttpPost request = new HttpPost(url + API_COLLECTIONS + "/" + id + "/upsert");
        request.setEntity(new StringEntity(document, StandardCharsets.UTF_8));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            logger.info("upsertDocument response body: {}", responseBody);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChromaCollection that = (ChromaCollection) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public String toString() {
        return "Collection [id=" + id + "," +
                " name=" + name + ", " +
                "tenant=" + tenant + ", " +
                "database=" + database + "]";
    }



}
