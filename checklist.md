# Implementation Checklist

## 1. Foundation

- [x] Initialize the repository as a `uv` Python project.
- [x] Pin a Python version supported by Spark.
- [x] Define environment variables and shared settings.
- [x] Create Docker Compose services for Kafka, PostgreSQL, Grafana, and Kafka UI.
- [x] Define the PostgreSQL schema and provision Grafana's data source.
- [x] Start Docker Desktop and verify each service is healthy.

## 2. Log Ingestion

- [x] Define a realistic web-server log event schema.
- [x] Build a repeatable normal-traffic generator.
- [x] Build controlled error-spike, suspicious-IP, and slow-endpoint scenarios.
- [x] Build a Kafka producer with configurable rate and scenario.
- [x] Verify messages reach Kafka by consuming the `web_logs` topic.

## 3. Streaming Analytics

- [x] Implement Kafka message parsing and input validation with Spark Structured Streaming.
- [x] Implement one-minute request, error-rate, endpoint, and IP metrics.
- [x] Implement error-spike, suspicious-IP, and slow-endpoint rules.
- [x] Implement PostgreSQL metric and alert writers.
- [x] Verify the full pipeline: Kafka → Spark → PostgreSQL.

## 4. Grafana Dashboard

- [x] Create requests-per-minute and error-rate panels.
- [x] Create top-endpoint and slow-endpoint panels.
- [x] Create the active-alert panel.
- [x] Configure a five-second dashboard refresh and alert-friendly thresholds.

## 5. Quality and Submission

- [x] Add automated tests for event generation and anomaly rules.
- [x] Test normal traffic and every anomaly scenario end-to-end.
- [ ] Capture architecture and dashboard screenshots.
- [ ] Write the report and demo script.
- [ ] Review the README setup instructions.
