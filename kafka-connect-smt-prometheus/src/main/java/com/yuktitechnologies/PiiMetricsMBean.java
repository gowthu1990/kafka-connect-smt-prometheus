package com.yuktitechnologies;

public interface PiiMetricsMBean {
    long getTotalMessagesProcessed();
    double getAverageProcessingLatencyMs();
}