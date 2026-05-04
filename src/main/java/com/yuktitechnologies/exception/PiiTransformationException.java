package com.yuktitechnologies.exception;

import org.apache.kafka.connect.errors.DataException;

/**
 * Custom exception to trigger specific alerting rules in Dynatrace/monitoring tools.
 * Extends DataException to ensure Kafka Connect safely routes the offending record
 * to the Dead Letter Queue (DLQ) instead of crashing the task.
 */
public class PiiTransformationException extends DataException {

    public PiiTransformationException(String message) {
        super(message);
    }

    public PiiTransformationException(String message, Throwable cause) {
        super(message, cause);
    }
}