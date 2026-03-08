package com.yuktitechnologies;

import java.util.concurrent.atomic.AtomicLong;

public class PiiMetrics implements PiiMetricsMBean {
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong totalLatencyNs = new AtomicLong(0);

    public void recordProcessingTime(long latencyNanoseconds) {
        totalMessages.incrementAndGet();
        totalLatencyNs.addAndGet(latencyNanoseconds);
    }

    @Override
    public long getTotalMessagesProcessed() {
        return totalMessages.get();
    }

    @Override
    public double getAverageProcessingLatencyMs() {
        long count = totalMessages.get();
        if (count == 0) return 0.0;
        // Convert nanoseconds to milliseconds
        return (double) totalLatencyNs.get() / count / 1_000_000.0;
    }
}