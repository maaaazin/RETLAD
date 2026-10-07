-- Run these queries in PostgreSQL while testing the Spark pipeline.

SELECT *
FROM request_metrics
ORDER BY window_start DESC
LIMIT 20;

SELECT *
FROM anomalies
ORDER BY detected_at DESC
LIMIT 20;

