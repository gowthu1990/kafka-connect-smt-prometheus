package com.yuktitechnologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yuktitechnologies.exception.PiiTransformationException;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Timer;
// Changed imports to JMX
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class PiiMaskingTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger logger = LoggerFactory.getLogger(PiiMaskingTransform.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String instanceId;

    // 1. Swapped to JMX Registry
    private JmxMeterRegistry jmxRegistry;

    // Core application timer
    private Timer smtProcessingTimer;

    @Override
    public void configure(Map<String, ?> configs) {
        this.instanceId = Integer.toHexString(System.identityHashCode(this));
        logger.info("Initializing PiiMaskingTransform instance [ID: {}]", instanceId);

        // 2. Initialize JmxMeterRegistry to expose metrics to Prometheus
        this.jmxRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        logger.info("JMX Telemetry Registry Initialized successfully.");

        // Build smt_processing_latency Timer
        this.smtProcessingTimer = Timer.builder("kafka.smt.processing.time")
                .description("Internal execution time of the Kafka Connect SMT")
                .publishPercentiles(0.95, 0.99)
                .register(jmxRegistry);
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            return record;
        }

        // 3. Start Micrometer Sample using the new JMX registry
        Timer.Sample smtSample = Timer.start(jmxRegistry);

        try {
            if (!(record.value() instanceof String)) {
                String errorMsg = String.format("Schema mismatch for record on topic [%s] partition [%s]. Expected String.",
                        record.topic(), record.kafkaPartition());
                logger.error("[DATA_VALIDATION_ALERT] {} Routing to DLQ.", errorMsg);
                throw new PiiTransformationException(errorMsg);
            }

            String rawJson = (String) record.value();
            JsonNode rootNode = MAPPER.readTree(rawJson);

            if (!rootNode.isObject()) {
                throw new PiiTransformationException("JSON payload is not an object.");
            }

            ObjectNode transformedValue = (ObjectNode) rootNode;

            // Core Business Logic: PII Masking
            maskSensitiveFields(transformedValue);

            long currentTimeMs = System.currentTimeMillis();
            transformedValue.put("processed_timestamp", currentTimeMs);

            // Defensive Extraction of Epochs
            Long cbsEpoch = extractEpochSafely(transformedValue, "cbs_generated_at");
            Long k1Epoch  = extractEpochSafely(transformedValue, "k1epoch");
            Long k2Epoch  = extractEpochSafely(transformedValue, "k2epoch");
            Long k3Epoch  = extractEpochSafely(transformedValue, "k3epoch");

            // 4. Update all conditional registrations to use jmxRegistry
            if (cbsEpoch != null && k1Epoch != null && k1Epoch >= cbsEpoch) {
                Timer.builder("kafka.pipeline.hop.cbs_to_k1")
                        .description("Latency from CBS to K1")
                        .publishPercentiles(0.95, 0.99)
                        .register(jmxRegistry)
                        .record(Duration.ofMillis(k1Epoch - cbsEpoch));
            }

            if (k1Epoch != null && k2Epoch != null && k2Epoch >= k1Epoch) {
                Timer.builder("kafka.pipeline.hop.k1_to_k2")
                        .description("Latency of K1 to K2 Transformation")
                        .publishPercentiles(0.95, 0.99)
                        .register(jmxRegistry)
                        .record(Duration.ofMillis(k2Epoch - k1Epoch));
            }

            if (k2Epoch != null && k3Epoch != null && k3Epoch >= k2Epoch) {
                Timer.builder("kafka.pipeline.hop.k2_to_k3")
                        .description("Latency of K2 to K3 Transformation")
                        .publishPercentiles(0.95, 0.99)
                        .register(jmxRegistry)
                        .record(Duration.ofMillis(k3Epoch - k2Epoch));
            }

            // Serialize back to String
            String updatedJson = MAPPER.writeValueAsString(transformedValue);

            // Stop the sample and record the SMT processing time automatically
            smtSample.stop(smtProcessingTimer);

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

    private Long extractEpochSafely(ObjectNode payload, String fieldName) {
        JsonNode node = payload.get(fieldName);
        if (node != null) {
            try {
                if (node.isNumber()) {
                    return node.asLong();
                } else if (node.isTextual()) {
                    return Long.parseLong(node.asText());
                }
            } catch (NumberFormatException e) {
                logger.debug("Malformed timestamp for field [{}]. Skipping metric calculation.", fieldName);
                return null;
            }
        }
        return null;
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
        if (jmxRegistry != null) {
            jmxRegistry.close();
        }
    }
}