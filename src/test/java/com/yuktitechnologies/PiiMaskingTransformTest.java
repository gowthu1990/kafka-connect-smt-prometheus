package com.yuktitechnologies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuktitechnologies.exception.PiiTransformationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class PiiMaskingTransformTest {

    private PiiMaskingTransform<SinkRecord> transform;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        transform = new PiiMaskingTransform<>();
        // Initialize the transform (which now spins up the JmxMeterRegistry)
        transform.configure(new HashMap<>());
    }

    @AfterEach
    void tearDown() {
        // This cleanly unregisters the JMX MBeans to prevent InstanceAlreadyExistsException
        transform.close();
    }

    @Test
    void testSuccessfulPiiMaskingAndMetricsProcessing() throws Exception {
        // Arrange: Create realistic epochs for the new AOV pipeline architecture
        long now = System.currentTimeMillis();
        long cbsEpoch = now - 5000;
        long k1Epoch = now - 4000;
        long k2Epoch = now - 1000;
        long k3Epoch = now - 10;

        // Inject the specific hop epochs expected by the new telemetry logic
        String rawJson = String.format(
                "{\"ssn\":\"123-456-7890\", \"amount\":250.50, \"cbs_generated_at\":%d, \"k1epoch\":%d, \"k2epoch\":%d, \"k3epoch\":%d}",
                cbsEpoch, k1Epoch, k2Epoch, k3Epoch
        );

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, Schema.STRING_SCHEMA, rawJson, 0L);

        // Act
        SinkRecord transformedRecord = transform.apply(record);

        // Assert
        assertTrue(transformedRecord.value() instanceof String);
        String transformedString = (String) transformedRecord.value();

        JsonNode resultNode = mapper.readTree(transformedString);
        assertEquals("***-**-****", resultNode.get("ssn").asText());
        assertEquals(250.50, resultNode.get("amount").asDouble());
        assertTrue(resultNode.has("processed_timestamp"));

        // Ensure the original epochs were safely preserved in the payload
        assertTrue(resultNode.has("cbs_generated_at"));
        assertTrue(resultNode.has("k3epoch"));
    }

    @Test
    void testDefensiveGuardrailsWithMissingAndMalformedEpochs() throws Exception {
        // Arrange: JSON missing k1epoch and containing a malformed k3epoch string
        String rawJson = "{\"ssn\":\"123-456-7890\", \"cbs_generated_at\":1714000000000, \"k2epoch\":1714000005000, \"k3epoch\":\"invalid_string_timestamp\"}";
        SinkRecord record = new SinkRecord("test-topic", 0, null, null, Schema.STRING_SCHEMA, rawJson, 0L);

        // Act
        // The transform should NOT crash, despite missing and bad timestamp data
        SinkRecord transformedRecord = transform.apply(record);

        // Assert
        assertTrue(transformedRecord.value() instanceof String);
        String transformedString = (String) transformedRecord.value();
        JsonNode resultNode = mapper.readTree(transformedString);

        // Verify core business logic (PII masking) still succeeded
        assertEquals("***-**-****", resultNode.get("ssn").asText());
        assertTrue(resultNode.has("processed_timestamp"));
    }

    @Test
    void testUnexpectedDataTypeThrowsCustomException() {
        SinkRecord record = new SinkRecord("test-topic", 0, null, null, Schema.INT32_SCHEMA, 12345, 0L);

        PiiTransformationException exception = assertThrows(PiiTransformationException.class, () -> {
            transform.apply(record);
        });
        assertTrue(exception.getMessage().contains("Expected String"));
    }

    @Test
    void testMalformedJsonThrowsException() {
        String badJson = "this is not valid json";
        SinkRecord record = new SinkRecord("test-topic", 0, null, null, Schema.STRING_SCHEMA, badJson, 0L);

        PiiTransformationException exception = assertThrows(PiiTransformationException.class, () -> {
            transform.apply(record);
        });
        assertTrue(exception.getMessage().contains("JSON parsing/serialization failure") || exception.getMessage().contains("JSON payload is not an object"));
    }

    @Test
    void testNullValueHandling() {
        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, null, 0L);

        SinkRecord transformedRecord = transform.apply(record);

        assertNull(transformedRecord.value());
    }
}