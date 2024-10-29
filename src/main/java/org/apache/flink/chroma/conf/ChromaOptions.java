package org.apache.flink.chroma.conf;

import java.util.Objects;

public class ChromaOptions extends ConnectionOptions {


    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_TENANT = "default_tenant";
    private static final String DEFAULT_DATABASE = "default_database";

    private final String tenant;
    private final String database;
    private final String collection;
    private final boolean autoCreateCollection;

    public ChromaOptions(String host,
                         int port,
                         String protocol,
                         AuthType authType,
                         String authIdentify,
                         String tenant,
                         String database,
                         String collection,
                         boolean autoCreateCollection) {
        super(host, port, protocol, authType, authIdentify);
        this.tenant = tenant;
        this.database = database;
        this.collection = collection;
        this.autoCreateCollection = autoCreateCollection;
    }


    public String getDatabase() {
        if (database == null) {
            return DEFAULT_DATABASE;
        }
        return database;
    }

    public String getTenant() {
        if (tenant == null) {
            return DEFAULT_TENANT;
        }
        return tenant;
    }

    public String getCollection() {
        return collection;
    }

    public boolean isAutoCreateCollection() {
        return autoCreateCollection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChromaOptions that = (ChromaOptions) o;
        return Objects.equals(host, that.host)
                && port == that.port
                && Objects.equals(protocol, that.protocol)
                && authType == that.authType
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                host, port, protocol, tenant, database, collection);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private int port;
        private String protocol;
        private AuthType authType;
        private String tenant;
        private String database;
        private String collection;
        private String authIdentify;
        private boolean autoCreateCollection;

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }
        public Builder setProtocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder setAuthType(AuthType authType) {
            this.authType = authType;
            return this;
        }
        public Builder setTenant(String tenant) {
            this.tenant = tenant;
            return this;
        }
        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }
        public Builder setCollection(String collection) {
            this.collection = collection;
            return this;
        }
        public Builder setAuthIdentify(String authIdentify) {
            this.authIdentify = authIdentify;
            return this;
        }
        public Builder setAutoCreateCollection(boolean autoCreateCollection) {
            this.autoCreateCollection = autoCreateCollection;
            return this;
        }
        public ChromaOptions build() {
            return new ChromaOptions(host, port, protocol, authType, authIdentify, tenant, database, collection, autoCreateCollection);
        }
    }
}
