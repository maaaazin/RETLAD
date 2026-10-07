# RETLAD — Real-Time Log Anomaly Detection

A Big Data Analytics course project that ingests simulated web-server logs with
Apache Kafka, analyzes them in real time using Spark Structured Streaming, and
visualizes operational metrics and anomalies in Grafana.

## Architecture

```text
Python log generator → Kafka → Spark Structured Streaming → PostgreSQL → Grafana
```

## Planned anomaly detection

- Error-rate spike within a one-minute window
- Excessive requests from one IP address
- Endpoint with unusually high average response time

## Local services

| Service | Address | Purpose |
| --- | --- | --- |
| Kafka | `localhost:29092` | Log-event ingestion |
| PostgreSQL | `localhost:5432` | Processed metrics and alerts |
| Grafana | `http://localhost:3000` | Monitoring dashboard |
| Kafka UI | `http://localhost:8080` | Kafka inspection during development |

## Setup

1. Copy `.env.example` to `.env` and replace the example passwords.
2. Install the locked Python environment with `uv sync`.
3. Start the local infrastructure with `docker compose up -d`.

Spark requires a supported Java runtime. Use Java 17 for local development.

Grafana provisions the **RETLAD: Real-Time Log Anomaly Detection** dashboard
automatically. Once the services are running, sign in at
`http://localhost:3000` with the credentials in `.env`.

## Generate test traffic

After Kafka is running, use `uv run` to send simulated web logs:

```bash
uv run python -m producer.kafka_producer --scenario normal
uv run python -m producer.kafka_producer --scenario error_spike --events-per-second 20
uv run python -m producer.kafka_producer --scenario suspicious_ip --events-per-second 60
uv run python -m producer.kafka_producer --scenario slow_endpoint --events-per-second 10
```

Use `--max-events 10` for a short, finite run. The Spark pipeline is added in
the next milestone.

### Recommended live demo

Keep normal traffic running in one terminal:

```bash
uv run python -m producer.kafka_producer --scenario normal --events-per-second 5
```

While it continues, use a second terminal to inject a short, finite anomaly
burst. Normal traffic is not stopped.

```bash
# Demonstrate an order-service failure.
uv run python -m producer.kafka_producer --scenario error_spike --events-per-second 20 --max-events 40

# Demonstrate excessive traffic from one IP address.
uv run python -m producer.kafka_producer --scenario suspicious_ip --events-per-second 60 --max-events 60
```

Every event contains a `traffic_type` label for explaining the demonstration.
It is not used by Spark's anomaly rules; Spark detects the actual error rate,
request volume, and response-time behaviour.

## Run streaming analytics

Once the local services are running, start the pipeline in a second terminal:

```bash
uv run python -m spark.streaming_processor
```

It consumes the `web_logs` Kafka topic, produces one-minute metrics and
anomaly alerts, and writes them to PostgreSQL.
