package com.yuktitechnologies;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PiiMaskingTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(PiiMaskingTransform.class);

    private PiiMetrics metrics;
    private JmxMetricsManager jmxManager;
    private String instanceId;

    @Override
    public void configure(Map<String, ?> configs) {
        this.instanceId = Integer.toHexString(System.identityHashCode(this));
        logger.info("Initializing PiiMaskingTransform instance [ID: {}]", instanceId);

        this.metrics = new PiiMetrics();
        this.jmxManager = new JmxMetricsManager();
        this.jmxManager.registerMetrics(metrics, instanceId);
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            return record; // Null records (tombstones) are safely bypassed
        }

        long startTimeNs = System.nanoTime();

        try {
            // STRICT CONTRACT: If it's not a Map, reject it and trigger an alert!
            if (!(record.value() instanceof Map)) {
                String errorMsg = String.format("Schema mismatch for record on topic [%s] partition [%s]. Expected Map.",
                        record.topic(), record.kafkaPartition());
                logger.error("[PII_PROCESSING_ALERT] {} Routing to DLQ.", errorMsg);
                throw new PiiTransformationException(errorMsg);
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> originalValue = (Map<String, Object>) record.value();
            Map<String, Object> transformedValue = new HashMap<>(originalValue);

            maskSensitiveFields(transformedValue);
            transformedValue.put("processed_timestamp", System.currentTimeMillis());

            metrics.recordProcessingTime(System.nanoTime() - startTimeNs);

            return record.newRecord(
                    record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    record.valueSchema(), transformedValue, record.timestamp()
            );

        } catch (PiiTransformationException e) {
            throw e; // Let our custom exception bubble up to the DLQ handler
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected failure masking PII for record on topic [%s] partition [%s].",
                    record.topic(), record.kafkaPartition());
            logger.error("[PII_PROCESSING_ALERT] {} Routing to DLQ.", errorMsg, e);
            throw new PiiTransformationException(errorMsg, e);
        }
    }

    private void maskSensitiveFields(Map<String, Object> payload) {
        if (payload.containsKey("ssn")) {
            payload.put("ssn", "***-**-****");
        }
        if (payload.containsKey("credit_card")) {
            payload.put("credit_card", "****-****-****-****");
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        logger.info("Shutting down PiiMaskingTransform instance [ID: {}]", instanceId);
        if (jmxManager != null) {
            jmxManager.unregisterMetrics();
        }
    }
}