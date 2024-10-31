package org.apache.flink.chroma;
import org.apache.flink.chroma.conf.AuthType;
import org.junit.jupiter.api.Test;

class ChromaClientTest {
    private final ChromaClient chromaClient = ChromaClient.builder().url("127.0.0.1:8000").build();

    @Test
    void testHealthCheck() {

    }

    @Test
    void testCheckDatabase() {
    }
}