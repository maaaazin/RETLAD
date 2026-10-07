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

The producer and Spark pipeline are added in the next milestones.
