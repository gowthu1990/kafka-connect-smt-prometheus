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

    // FIX: Using the concrete SinkRecord instead of the wildcard ConnectRecord
    private PiiMaskingTransform<SinkRecord> transform;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        transform = new PiiMaskingTransform<>();
        transform.configure(new HashMap<>());
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    @Test
    void testSuccessfulPiiMasking() throws Exception {
        String rawJson = "{\"ssn\":\"123-456-7890\", \"amount\":250.50, \"eventTimestamp\":1700000000000}";
        SinkRecord record = new SinkRecord("test-topic", 0, null, null,
                Schema.STRING_SCHEMA, rawJson, 0L);

        SinkRecord transformedRecord = transform.apply(record);

        assertInstanceOf(String.class, transformedRecord.value());
        String transformedString = (String) transformedRecord.value();

        JsonNode resultNode = mapper.readTree(transformedString);
        assertEquals("***-**-****", resultNode.get("ssn").asText());
        assertEquals(250.50, resultNode.get("amount").asDouble());
        assertTrue(resultNode.has("processed_timestamp"));
    }

    @Test
    void testUnexpectedDataTypeThrowsCustomException() {
        SinkRecord record = new SinkRecord("test-topic", 0, null, null,
                Schema.INT32_SCHEMA, 12345, 0L);

        PiiTransformationException exception = assertThrows(PiiTransformationException.class, () -> {
            transform.apply(record);
        });
        assertTrue(exception.getMessage().contains("Expected String"));
    }

    @Test
    void testMalformedJsonThrowsException() {
        String badJson = "this is not valid json";
        SinkRecord record = new SinkRecord("test-topic", 0, null, null,
                Schema.STRING_SCHEMA, badJson, 0L);

        PiiTransformationException exception = assertThrows(PiiTransformationException.class, () -> {
            transform.apply(record);
        });
        assertTrue(exception.getMessage().contains("JSON parsing/serialization failure")
                || exception.getMessage().contains("JSON payload is not an object"));
    }

    @Test
    void testNullValueHandling() {
        SinkRecord record = new SinkRecord("test-topic", 0, null, null,
                null, null, 0L);

        SinkRecord transformedRecord = transform.apply(record);

        assertNull(transformedRecord.value());
    }
}