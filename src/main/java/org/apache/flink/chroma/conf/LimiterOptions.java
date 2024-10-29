package org.apache.flink.chroma.conf;

import java.io.Serializable;

public class LimiterOptions implements Serializable {
    private final long maxTokens;
    private final long refillInterval;
    private final long tokensPerInterval;

    public LimiterOptions(long maxTokens, long refillInterval, long tokensPerInterval) {
        this.maxTokens = maxTokens;
        this.refillInterval = refillInterval;
        this.tokensPerInterval = tokensPerInterval;
    }

    public long getTokensPerInterval() {
        return tokensPerInterval;
    }

    public long getRefillInterval() {
        return refillInterval;
    }

    public long getMaxTokens() {
        return maxTokens;
    }
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long maxTokens;
        private long refillInterval;
        private long tokensPerInterval;
        public Builder maxTokens(long maxTokens) {
            this.maxTokens = maxTokens;
            return this;
        }
        public Builder refillInterval(long refillInterval) {
            this.refillInterval = refillInterval;
            return this;
        }
        public Builder tokensPerInterval(long tokensPerInterval) {
            this.tokensPerInterval = tokensPerInterval;
            return this;
        }
        public LimiterOptions build() {
            return new LimiterOptions(maxTokens, refillInterval, tokensPerInterval);
        }
    }
}
