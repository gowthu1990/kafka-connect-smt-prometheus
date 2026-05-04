package com.yuktitechnologies;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class JsonLoadGenerator {

    // A pre-allocated block of text to simulate "Fat Records" without slowing down the generator itself
    private static final String FAT_RECORD_PADDING = "X".repeat(5000);

    public static void main(String[] args) throws InterruptedException {
        int totalMessages = 1800000;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High-throughput Producer Tuning
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "json-test-topic";

        System.out.println("Starting Enterprise JSON Load Generator at ~30,000 TPS with Simulated Jitter...");

        int batchSize = 3000;
        long sleepTimeMs = 100;

        for (int i = 1; i <= totalMessages; i++) {
            long now = System.currentTimeMillis();

            // Generate the payload with built-in variance
            String jsonPayload = getJsonPayload(now, i);

            producer.send(new ProducerRecord<>(topic, null, jsonPayload));

            if (i % 100000 == 0) {
                System.out.println("Produced " + i + " messages...");
            }

            if (i % batchSize == 0) {
                Thread.sleep(sleepTimeMs);
            }
        }

        producer.close();
        System.out.println("High-volume load generation complete.");
    }

    private static String getJsonPayload(long now, int i) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // 1. Network Jitter Simulation: Add random variance (-50ms to +50ms) to upstream hops
        long cbsEpoch = now - 8000 + random.nextLong(-200, 200);
        long k1Epoch  = now - 4000 + random.nextLong(-50, 50);
        long k2Epoch  = now - 1000 + random.nextLong(-20, 20);
        long k3Epoch  = now - 10   + random.nextLong(-5, 5);

        // 2. CPU Processing Variance Simulation: 5% of records become "Fat Records"
        // This forces the JVM to allocate significantly more memory for the Jackson ObjectNode,
        // triggering minor Garbage Collection events that create realistic P99 tail latencies.
        // 2. CPU Processing Variance Simulation: 5% of records become "Fat Records"
        String optionalPadding = "";
        if (random.nextDouble() > 0.95) {
            // FIX: Removed the rogue leading quote, added the missing comma,
            // and properly closed the quote at the end of the padding string.
            optionalPadding = ", \"metadata_padding\": \"" + FAT_RECORD_PADDING + "\"";
        }

        return String.format(
                "{\"account_id\": \"ACC-%d\", \"ssn\": \"111-22-3333\", " +
                        "\"cbs_generated_at\": %d, \"k1epoch\": %d, \"k2epoch\": %d, \"k3epoch\": %d, " +
                        "\"amount\": %.2f%s}",
                i, cbsEpoch, k1Epoch, k2Epoch, k3Epoch, random.nextDouble(10.0, 5000.0), optionalPadding
        );
    }
}