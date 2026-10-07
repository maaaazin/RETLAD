# RETLAD User Guide

This guide explains how to run, demonstrate, stop, and restart the Real-Time
Log Anomaly Detection project.

## What runs in the project

| Component | What it does | How it runs |
| --- | --- | --- |
| Kafka | Receives generated web-server logs | Docker |
| PostgreSQL | Stores calculated metrics and alerts | Docker |
| Grafana | Shows the live dashboard | Docker |
| Kafka UI | Lets you inspect Kafka topics | Docker |
| Spark | Reads Kafka logs, calculates metrics, detects anomalies | Terminal |
| Producer | Generates normal traffic or a short anomaly burst | Terminal |

## First-time setup

From the project folder:

```bash
uv sync
cp .env.example .env
```

The default local Grafana credentials are:

```text
Username: admin
Password: change_this_password
```

You may change passwords in `.env` before starting Docker.

## Start the local services

Make sure Docker Desktop is running, then run:

```bash
docker compose up -d
docker compose ps
```

`docker compose ps` should show Kafka, PostgreSQL, Grafana, and Kafka UI as
running. Kafka and PostgreSQL should become healthy.

Open these pages:

- Grafana dashboard: <http://localhost:3000>
- Kafka UI: <http://localhost:8080>

## Start Spark

Open a terminal in the project folder. Spark needs Java 17:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
uv run python -m spark.streaming_processor
```

Leave this terminal running. Spark continuously reads logs from Kafka and
writes calculated metrics and alerts to PostgreSQL.

## Start normal traffic

Open a second terminal in the project folder:

```bash
uv run python -m producer.kafka_producer --scenario normal --events-per-second 5
```

Leave this terminal running. It simulates ordinary website visitors.

## Demonstrate an anomaly

Keep normal traffic running. Open a **third terminal** and run one finite
injection command. The injection ends automatically and normal traffic keeps
running.

### Error spike

Simulates repeated server failures in the order service:

```bash
uv run python -m producer.kafka_producer --scenario error_spike --events-per-second 20 --max-events 40
```

Expected Grafana result: a higher error-rate chart and an `error_rate_spike`
alert.

### Suspicious IP burst

Simulates excessive traffic from one IP address:

```bash
uv run python -m producer.kafka_producer --scenario suspicious_ip --events-per-second 60 --max-events 60
```

Expected Grafana result: `203.0.113.42` rises in **Top IP Addresses by
Requests**, and a `suspicious_ip_traffic` alert appears.

### Slow endpoint

Simulates a slow reporting API:

```bash
uv run python -m producer.kafka_producer --scenario slow_endpoint --events-per-second 20 --max-events 20
```

Expected Grafana result: `/api/reports` rises in **Slowest Endpoints**, and a
`slow_endpoint` alert appears.

Grafana refreshes every five seconds. Spark aggregates one-minute windows, so
allow about 15–30 seconds after an injection for charts and alerts to update.

## Stop everything safely

1. In the normal-traffic terminal, press `Ctrl+C`.
2. In the Spark terminal, press `Ctrl+C`.
3. Stop Kafka, PostgreSQL, Grafana, and Kafka UI:

```bash
docker compose down
```

This stops and removes the Docker containers but **preserves** your local
PostgreSQL and Grafana data. The next start reuses that data.

## Start everything again

After a normal shutdown, run the same sequence:

```bash
docker compose up -d
```

Then start Spark, start normal traffic, and optionally inject an anomaly using
the commands above.

## Delete all local project data

Only use this when you want a completely fresh demonstration. It removes the
local PostgreSQL and Grafana data volumes:

```bash
docker compose down -v
```

After this command, start services again with `docker compose up -d`. The
database schema and dashboard are created automatically from the project
files.

## Quick troubleshooting

### Grafana shows no data

- Confirm Spark is still running.
- Confirm normal traffic is running.
- In Grafana, choose **Last 30 minutes** and refresh the page.
- Wait up to 30 seconds after sending traffic.

### Docker services do not start

- Start Docker Desktop.
- Run `docker compose ps` to check service state.
- View a service log, for example: `docker compose logs kafka`.

### Spark reports a Java issue

Run the Java 17 `export` commands shown in **Start Spark**, then run Spark
again.
