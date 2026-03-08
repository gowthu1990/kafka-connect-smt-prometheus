# Kafka Connect SMT Performance Benchmarking (JSON Pipeline)

This repository provides a complete, observable local environment for benchmarking the compute overhead of Single Message Transformations (SMTs) in Kafka Connect.

This project proves the performance impact of deserializing, mutating, and re-serializing unstructured JSON string payloads on the fly using Jackson.

## Architecture Overview
* **Message Broker:** Confluent Kafka (Local)
* **Compute:** Kafka Connect (Standalone/Distributed worker)
* **Custom SMT:** `PiiMaskingTransform` (Parses JSON, masks PII, injects timestamps, calculates latencies)
* **Metrics Engine:** Prometheus (Scraping via JMX Java Agent on port `7071`)
* **Visualization:** Grafana (Dashboards for TPS, SMT Latency, End-to-End Latency)

## Prerequisites
* Docker & Docker Compose
* Java 11+ (JDK)
* Gradle (via Wrapper)
* curl

---

## Step 1: Infrastructure Setup

Start the complete infrastructure suite. This will spin up Kafka, Kafka Connect, Prometheus, and Grafana on the `kafka-connect-smt-prometheus_default` Docker bridge network.

```bash
docker compose up -d
```
Wait approximately 60 seconds for the Kafka Connect JVM to fully initialize the JMX agent. Verify the Connect REST API is up:

```bash
curl -s http://localhost:8083/ | grep version
```

---

## Step 2: Testing

The core SMT logic is located in src/main/java/com/yuktitechnologies/PiiMaskingTransform.java. Whenever you modify the transformation logic, always run the unit tests to ensure the JSON string contract remains intact:

```bash
./gradlew clean test
```

---

## Step 3: Deployment Automation (deploy.sh)
To streamline rebuilding and deploying the Fat JAR to the Connect worker, create a file named deploy.sh in the root of your project with the following contents:

```bash
#!/bin/bash
echo "==> Cleaning and Building Fat JAR..."
./gradlew clean jar

echo "==> Copying JAR to plugins directory..."
cp /build/libs/pii-masking-smt.jar ./plugins/yuktitechnologies-smt/

echo "==> Restarting Kafka Connect container..."
docker compose restart local-connect

echo "==> Waiting for Kafka Connect API to become available..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083/)" != "200" ]]; do
  sleep 5
  echo "Waiting..."
done

echo "==> Deployment Complete!"
```

Make the script executable and run it:

```bash
chmod +x deploy.sh
./deploy.sh
```

---

## Step 4: Configure the Connector
Because this benchmark simulates raw JSON strings arriving from upstream, you must use the StringConverter so the SMT receives raw text to parse via Jackson.

Register or update the connector using the Kafka Connect REST API:

```bash
curl -X PUT http://localhost:8083/connectors/json-pipeline/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "json-test-topic",
    "file": "/tmp/json-output.txt",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "transforms": "Masking",
    "transforms.Masking.type": "com.yuktitechnologies.PiiMaskingTransform"
  }'
```

Verify the connector is RUNNING:

```bash
curl -s http://localhost:8083/connectors/json-pipeline/status
```

---

## Step 5: Generate Load

Run the high-throughput load generator from your IDE or via command line. The `JsonLoadGenerator.java` uses micro-batching to bypass OS thread scheduling limits, easily pushing **1,000+ TPS**.

Ensure the generator targets the `json-test-topic`.

---

## Step 6: Observability & Grafana

Once the load generator is running, data will flow into Prometheus and become visible in Grafana.

[Image of Grafana dashboard displaying time series metrics for high throughput systems]

### 1. Connect Grafana to Prometheus
1. Open Grafana at `http://localhost:3000` (admin/admin).
2. Go to **Connections > Data Sources > Add Data Source**.
3. Select **Prometheus**.
4. Set the URL to the internal Docker network hostname: `http://local-prometheus:9090`.
5. Click **Save & Test**.

### 2. Configure Benchmark Dashboards
Create a new dashboard with three Time Series panels using the following PromQL queries. Set the **Min Step** to `15s` and the **Legend** to `{{id}}`.

**A. Throughput (Messages/Sec)**
* *Query:* `rate(com_yuktitechnologies_jsonmetrics_totalmessagesprocessed[1m])`
* *Unit:* Short

**B. SMT Processing Overhead (Avg ms per message)**
* *Description:* The exact compute cost of parsing, masking, and serializing the JSON string.
* *Query:* `rate(com_yuktitechnologies_jsonmetrics_totalsmtprocessinglatencyns[1m]) / rate(com_yuktitechnologies_jsonmetrics_totalmessagesprocessed[1m]) / 1000000`
* *Unit:* Time > Milliseconds (ms)

**C. End-to-End Latency (Avg ms)**
* *Description:* Total time-in-flight from source creation to sink consumption.
* *Query:* `rate(com_yuktitechnologies_jsonmetrics_totalendtoendms[1m]) / rate(com_yuktitechnologies_jsonmetrics_totalmessagesprocessed[1m])`
* *Unit:* Time > Milliseconds (ms)

---

## Step 7: Benchmark Results (Local Profile)

*(Note to Architect: Run your load generator for at least 5 minutes to stabilize the JVM, then record the average metrics from Grafana below before presenting.)*

* **Target Load:** ~1,000 TPS
* **Average Throughput Achieved:** `~900`
* **Average SMT Latency (Jackson Overhead):** `0.02 ms`
* **Average End-to-End Latency:** `~310 ms`

![img.png](img.png)

---

## Troubleshooting

* Prometheus Target is DOWN: Check http://localhost:9090/targets. If local-connect is down, ensure the JMX Java Agent in docker-compose.yml (KAFKA_OPTS) is bound to port 7071 and Prometheus is scraping the correct Docker hostname (local-connect:7071).

* Grafana shows "No Data": Ensure your time range is set to "Last 15 minutes" or click the "Instant" query toggle to verify the data connection.

* SMT Throws Exception: If the SMT throws a [DATA_VALIDATION_ALERT], you likely left the value.converter as JsonConverter instead of StringConverter. Check your connector config.