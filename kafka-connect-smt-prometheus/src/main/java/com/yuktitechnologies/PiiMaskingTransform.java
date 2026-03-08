package com.yuktitechnologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yuktitechnologies.exception.PiiTransformationException;
import com.yuktitechnologies.jmx.JmxMetricsManager;
import com.yuktitechnologies.jmx.PiiMetrics;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PiiMaskingTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(PiiMaskingTransform.class);

    // Thread-safe, reused instance for high performance
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PiiMetrics metrics;
    private JmxMetricsManager jmxManager;
    private String instanceId;

    @Override
    public void configure(Map<String, ?> configs) {
        this.instanceId = Integer.toHexString(System.identityHashCode(this));
        logger.info("Initializing PiiMaskingTransform instance [ID: {}]", instanceId);

        this.metrics = new PiiMetrics();
        this.jmxManager = new JmxMetricsManager();
        this.jmxManager.registerMetrics(metrics, instanceId + "_JSON");
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            return record;
        }

        long smtStartTimeNs = System.nanoTime();

        try {
            if (!(record.value() instanceof String)) {
                String errorMsg = String.format("Schema mismatch for record on topic [%s] partition [%s]. " +
                                "Expected String.", record.topic(), record.kafkaPartition());
                logger.error("[DATA_VALIDATION_ALERT] {} Routing to DLQ.", errorMsg);
                throw new PiiTransformationException(errorMsg);
            }

            String rawJson = (String) record.value();
            JsonNode rootNode = MAPPER.readTree(rawJson);

            if (!rootNode.isObject()) {
                throw new PiiTransformationException("JSON payload is not an object.");
            }

            ObjectNode transformedValue = (ObjectNode) rootNode;

            // 1. PII Masking
            maskSensitiveFields(transformedValue);
            transformedValue.put("processed_timestamp", System.currentTimeMillis());

            // 2. Extract Generic Epochs safely
            long eventTimestamp = extractEpoch(transformedValue, "eventTimestamp");
            long firstHopTimestamp = extractEpoch(transformedValue, "firstHopTimestamp");
            long finalHopTimestamp = extractEpoch(transformedValue, "finalHopTimestamp");
            long currentTimeMs = System.currentTimeMillis();

            // 3. Calculate Component-Level Latencies
            long sourceToFirstHop = firstHopTimestamp - eventTimestamp;
            long interHopLatency = finalHopTimestamp - firstHopTimestamp;
            long sinkConsumptionLag = currentTimeMs - finalHopTimestamp;
            long endToEndLatency = currentTimeMs - eventTimestamp;

            // 4. Diagnostic Threshold Logging
            long THRESHOLD_MS = 500L;
            if (sourceToFirstHop > THRESHOLD_MS) {
                logger.warn("[LATENCY_THRESHOLD_ALERT] High Source Ingest Latency: {} ms on partition {}",
                        sourceToFirstHop, record.kafkaPartition());
            }
            if (sinkConsumptionLag > THRESHOLD_MS) {
                logger.warn("[LATENCY_THRESHOLD_ALERT] High Sink Consumption Lag: {} ms on partition {}",
                        sinkConsumptionLag, record.kafkaPartition());
            }

            // 5. Serialize back to String
            String updatedJson = MAPPER.writeValueAsString(transformedValue);

            // 6. Record all metrics to JMX
            long smtLatencyNs = System.nanoTime() - smtStartTimeNs;
            metrics.recordMetrics(smtLatencyNs, sourceToFirstHop, interHopLatency, endToEndLatency);

            return record.newRecord(
                    record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    record.valueSchema(), updatedJson, record.timestamp()
            );

        } catch (JsonProcessingException e) {
            String errorMsg = String.format("JSON parsing/serialization failure on topic [%s] partition [%s].",
                    record.topic(), record.kafkaPartition());
            logger.error("[DATA_VALIDATION_ALERT] {} Routing to DLQ.", errorMsg, e);
            throw new PiiTransformationException(errorMsg, e);
        } catch (PiiTransformationException e) {
            throw e;
        } catch (Exception e) {
            String errorMsg = String.format("Unexpected failure processing record on topic [%s] partition [%s].",
                    record.topic(), record.kafkaPartition());
            logger.error("[OBSERVABILITY_PROCESSING_ALERT] {} Routing to DLQ.", errorMsg, e);
            throw new PiiTransformationException(errorMsg, e);
        }
    }

    private long extractEpoch(ObjectNode payload, String fieldName) {
        JsonNode node = payload.get(fieldName);
        if (node != null && node.isNumber()) {
            return node.asLong();
        }
        return System.currentTimeMillis(); // Fallback to avoid crashing on malformed data
    }

    private void maskSensitiveFields(ObjectNode payload) {
        if (payload.has("ssn")) {
            payload.put("ssn", "***-**-****");
        }
        if (payload.has("credit_card")) {
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