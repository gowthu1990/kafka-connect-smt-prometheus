package com.yuktitechnologies.jmx;

import java.util.concurrent.atomic.AtomicLong;

public class PiiMetrics implements PiiMetricsMBean {
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong totalSmtLatencyNs = new AtomicLong(0);

    private final AtomicLong totalSourceToFirstHopMs = new AtomicLong(0);
    private final AtomicLong totalInterHopMs = new AtomicLong(0);
    private final AtomicLong totalEndToEndMs = new AtomicLong(0);

    public void recordMetrics(long smtLatencyNs, long sourceToFirstHop, long interHop, long endToEnd) {
        totalMessages.incrementAndGet();
        totalSmtLatencyNs.addAndGet(smtLatencyNs);
        totalSourceToFirstHopMs.addAndGet(sourceToFirstHop);
        totalInterHopMs.addAndGet(interHop);
        totalEndToEndMs.addAndGet(endToEnd);
    }

    @Override
    public long getTotalMessagesProcessed() {
        return totalMessages.get();
    }

    @Override
    public long getTotalSmtProcessingLatencyNs() {
        return totalSmtLatencyNs.get();
    }

    @Override
    public long getTotalSourceToFirstHopMs() {
        return totalSourceToFirstHopMs.get();
    }

    @Override
    public long getTotalInterHopMs() {
        return totalInterHopMs.get();
    }

    @Override
    public long getTotalEndToEndMs() {
        return totalEndToEndMs.get();
    }
}