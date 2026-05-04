package com.yuktitechnologies.jmx;

public interface PiiMetricsMBean {
    long getTotalMessagesProcessed();
    long getTotalSmtProcessingLatencyNs();

    // Raw accumulations, no averages
    long getTotalSourceToFirstHopMs();
    long getTotalInterHopMs();
    long getTotalEndToEndMs();
}