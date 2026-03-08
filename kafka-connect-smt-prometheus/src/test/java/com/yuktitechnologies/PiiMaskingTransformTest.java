package com.yuktitechnologies;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PiiMaskingTransformTest {

    private static final Logger logger = LoggerFactory.getLogger(PiiMaskingTransformTest.class);

    private PiiMaskingTransform<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        logger.info("Initializing test environment...");
        transform = new PiiMaskingTransform<>();
        transform.configure(new HashMap<>());
    }

    @AfterEach
    void tearDown() {
        logger.info("Tearing down test environment...");
        transform.close();
    }

    @Test
    void testSuccessfulPiiMasking() {
        logger.info("Executing test: testSuccessfulPiiMasking");

        Map<String, Object> payload = new HashMap<>();
        payload.put("ssn", "123-45-6789");
        payload.put("safe_field", "data");

        SourceRecord record = new SourceRecord(null, null, "test-topic", 0, null, null, null, payload);
        SourceRecord result = transform.apply(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> resultValue = (Map<String, Object>) result.value();

        assertEquals("***-**-****", resultValue.get("ssn"));
        assertEquals("data", resultValue.get("safe_field"));
        assertNotNull(resultValue.get("processed_timestamp"));

        logger.info("testSuccessfulPiiMasking completed successfully.");
    }

    @Test
    void testNullValueHandling() {
        logger.info("Executing test: testNullValueHandling");

        SourceRecord record = new SourceRecord(null, null, "test-topic", 0, null, null, null, null);
        SourceRecord result = transform.apply(record);

        assertNull(result.value(), "Null records should be bypassed without exception.");
        logger.info("testNullValueHandling completed successfully.");
    }

    @Test
    void testUnexpectedDataTypeThrowsCustomException() {
        logger.info("Executing test: testUnexpectedDataTypeThrowsCustomException");

        // Passing a String instead of a Map to trigger the exception handling
        SourceRecord badRecord = new SourceRecord(null, null, "test-topic", 0, null, null, null, "Not a Map");

        PiiTransformationException thrown = assertThrows(
                PiiTransformationException.class,
                () -> transform.apply(badRecord),
                "Expected a PiiTransformationException to be thrown for invalid payload types."
        );

        assertTrue(thrown.getMessage().contains("Schema mismatch for record"));
        logger.info("testUnexpectedDataTypeThrowsCustomException completed successfully. Custom Exception correctly caught.");
    }
}