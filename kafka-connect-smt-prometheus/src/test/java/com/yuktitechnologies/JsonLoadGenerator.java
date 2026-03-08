package com.yuktitechnologies;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

public class JsonLoadGenerator {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5"); // Wait 5ms to group messages together
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536"); // 64KB batch size
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Compress batches

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "json-test-topic";
        int totalMessages = 60000; // 60 seconds of continuous load

        System.out.println("Starting JSON Load Generator at ~1000 TPS...");

        int batchSize = 100; // Send 100 messages at a time
        long sleepTimeMs = 100; // Then sleep for 100ms (100 msgs / 100ms = 1000 TPS)

        for (int i = 1; i <= totalMessages; i++) {
            long now = System.currentTimeMillis();
            String jsonPayload = getJsonPayload(now, i);

            producer.send(new ProducerRecord<>(topic, null, jsonPayload));

            // Log progress
            if (i % 5000 == 0) {
                System.out.println("Produced " + i + " messages...");
            }

            // Sleep only after a batch is sent
            if (i % batchSize == 0) {
                Thread.sleep(sleepTimeMs);
            }
        }

        producer.close();
        System.out.println("Load generation complete. Baseline established.");
    }

    @NotNull
    private static String getJsonPayload(long now, int i) {
        long eventTimestamp = now - 300;       // Source created it 300ms ago
        long firstHopTimestamp = now - 200;    // Hit the first topic 200ms ago
        long finalHopTimestamp = now - 50;     // Hit the final topic 50ms ago

        return String.format(
                "{\"account_id\": \"ACC-%d\", \"ssn\": \"111-22-3333\", \"eventTimestamp\": %d, " +
                        "\"firstHopTimestamp\": %d, \"finalHopTimestamp\": %d, \"amount\": 250.50}",
                i, eventTimestamp, firstHopTimestamp, finalHopTimestamp
        );
    }
}