package org.apache.flink.chroma.sink.entity;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;
import java.util.Map;

public class ChromaSinkData {
    @JSONField(name = "id")
    private final String id;
    @JSONField(name = "uri")
    private final String uri;
    @JSONField(name = "embedding")
    private final List<Integer> embedding;
    @JSONField(name = "document")
    private final String document;
    @JSONField(name = "metadata")
    private final Map<String, Object> metadata;

    public ChromaSinkData(String id, String uri, List<Integer> embedding, String document, Map<String, Object> metadata) {
        this.id = id;
        this.uri = uri;
        this.embedding = embedding;
        this.document = document;
        this.metadata = metadata;
    }

    public String getId() {
        return id;
    }

    public String getUri() {
        return uri;
    }

    public List<Integer> getEmbedding() {
        return embedding;
    }

    public String getDocument() {
        return document;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String uri;
        private List<Integer> embedding;
        private String document;
        private Map<String, Object> metadata;
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }
        public Builder embedding(List<Integer> embedding) {
            this.embedding = embedding;
            return this;
        }
        public Builder document(String document) {
            this.document = document;
            return this;
        }
        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }
        public ChromaSinkData build() {
            return new ChromaSinkData(id, uri, embedding, document, metadata);
        }
    }

}
