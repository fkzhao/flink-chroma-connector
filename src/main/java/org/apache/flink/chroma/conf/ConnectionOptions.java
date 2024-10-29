package org.apache.flink.chroma.conf;

import java.io.Serializable;

public class ConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_PROTOCOL = "http";
    private static final AuthType DEFAULT_AUTH_TYPE = AuthType.AuthTypeNone;


    protected String host;
    protected int port;
    protected String protocol;
    protected AuthType authType;
    protected String authIdentity;

    public String getProtocol() {
        if (protocol == null) {
            return DEFAULT_PROTOCOL;
        }
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public AuthType getAuthType() {
        if (authType == null) {
            return DEFAULT_AUTH_TYPE;
        }
        return authType;
    }

    public void setAuthType(AuthType authType) {
        this.authType = authType;
    }

    public String getAuthIdentity() {
        return authIdentity;
    }

    public void setAuthIdentity(String authIdentity) {
        this.authIdentity = authIdentity;
    }

    ConnectionOptions(String host, int port, String protocol, AuthType authType, String authIdentity) {
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.authType = authType;
        this.authIdentity = authIdentity;
    }

    public String getConnectionUrl() {
        return String.format("%s://%s:%d", getProtocol(), host, port);
    }
}
