package org.apache.flink.chroma.sink.limit;

import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {

    private final long maxTokens;
    private final long refillInterval;
    private final long tokensPerInterval;
    private AtomicLong availableTokens;
    private AtomicLong lastRefillTime;


    public RateLimiter(long tokensPerInterval, long refillInterval, long maxTokens) {
        this.tokensPerInterval = tokensPerInterval;
        this.refillInterval = refillInterval;
        this.maxTokens = maxTokens;
        this.availableTokens = new AtomicLong(maxTokens);
        this.lastRefillTime = new AtomicLong(System.currentTimeMillis());
    }

    public synchronized boolean tryAcquire() {
        refillTokens();

        if (availableTokens.get() > 0) {
            availableTokens.decrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    private void refillTokens() {
        long currentTime = System.currentTimeMillis();
        long timeSinceLastRefill = currentTime - lastRefillTime.get();

        if (timeSinceLastRefill >= refillInterval) {
            long tokensToAdd = (timeSinceLastRefill / refillInterval) * tokensPerInterval;
            long newTokenCount = Math.min(availableTokens.get() + tokensToAdd, maxTokens);
            availableTokens.set(newTokenCount);
            lastRefillTime.set(currentTime);
        }
    }

    public long getAvailableTokens() {
        refillTokens();
        return availableTokens.get();
    }
}
