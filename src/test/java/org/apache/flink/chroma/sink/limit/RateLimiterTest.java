package org.apache.flink.chroma.sink.limit;

import org.junit.jupiter.api.Test;

public class RateLimiterTest {
    RateLimiter rateLimiter = new RateLimiter(10, 60*1000, 100);
    @Test
    public void testRateLimiter() {
        for (int i = 0; i < 1000; i++) {
            if (rateLimiter.tryAcquire()) {
                System.out.println(i);
            } else {
                System.out.println("Exceeding the limit");
            }
        }
    }
}
