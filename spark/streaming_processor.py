"""Run the Kafka-to-PostgreSQL Spark Structured Streaming pipeline."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from config.settings import settings
from spark.anomaly_detector import (
    detect_error_spikes,
    detect_slow_endpoints,
    detect_suspicious_ips,
)
from spark.database_writer import (
    write_anomalies,
    write_endpoint_metrics,
    write_ip_metrics,
    write_request_metrics,
)
from spark.transformations import create_metric_streams, parse_kafka_events


KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def start_query(dataframe: object, writer: object, checkpoint_name: str) -> object:
    """Start one recoverable foreach-batch query."""
    checkpoint = Path(settings.spark_checkpoint_dir, checkpoint_name).resolve()
    return (
        dataframe.writeStream.outputMode("update")
        .foreachBatch(writer)
        .option("checkpointLocation", str(checkpoint))
        .start()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("real-time-log-anomaly-detection")
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    metrics = create_metric_streams(parse_kafka_events(kafka_events))
    alerts = detect_error_spikes(metrics.requests).unionByName(
        detect_suspicious_ips(metrics.ips)
    ).unionByName(detect_slow_endpoints(metrics.endpoints))

    queries = [
        start_query(metrics.requests, write_request_metrics, "request_metrics"),
        start_query(metrics.endpoints, write_endpoint_metrics, "endpoint_metrics"),
        start_query(metrics.ips, write_ip_metrics, "ip_metrics"),
        start_query(alerts, write_anomalies, "anomalies"),
    ]
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
