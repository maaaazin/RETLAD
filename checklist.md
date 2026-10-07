# Implementation Checklist

## 1. Foundation

- [x] Initialize the repository as a `uv` Python project.
- [x] Pin a Python version supported by Spark.
- [x] Define environment variables and shared settings.
- [x] Create Docker Compose services for Kafka, PostgreSQL, Grafana, and Kafka UI.
- [x] Define the PostgreSQL schema and provision Grafana's data source.
- [ ] Start Docker Desktop and verify each service is healthy.

## 2. Log Ingestion

- [x] Define a realistic web-server log event schema.
- [x] Build a repeatable normal-traffic generator.
- [x] Build controlled error-spike, suspicious-IP, and slow-endpoint scenarios.
- [x] Build a Kafka producer with configurable rate and scenario.
- [ ] Verify messages in Kafka UI.

## 3. Streaming Analytics

- [ ] Read and parse Kafka messages with Spark Structured Streaming.
- [ ] Validate input and handle malformed messages.
- [ ] Calculate one-minute request, error-rate, endpoint, and IP metrics.
- [ ] Detect error spikes, suspicious-IP traffic, and slow endpoints.
- [ ] Persist metrics and alerts to PostgreSQL.

## 4. Grafana Dashboard

- [ ] Create requests-per-minute and error-rate panels.
- [ ] Create status-code, top-endpoint, and top-IP panels.
- [ ] Create slow-endpoint and active-alert panels.
- [ ] Configure dashboard refresh and alert-friendly thresholds.

## 5. Quality and Submission

- [ ] Add automated tests for event generation and anomaly rules.
- [ ] Test normal traffic and every anomaly scenario end-to-end.
- [ ] Capture architecture and dashboard screenshots.
- [ ] Write the report and demo script.
- [ ] Review the README setup instructions.

