"""Spark schemas and streaming aggregations for web-server logs."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


LOG_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), nullable=False),
        StructField("ip_address", StringType(), nullable=False),
        StructField("method", StringType(), nullable=False),
        StructField("endpoint", StringType(), nullable=False),
        StructField("status_code", IntegerType(), nullable=False),
        StructField("response_time_ms", IntegerType(), nullable=False),
        StructField("user_agent", StringType(), nullable=True),
        StructField("traffic_type", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class MetricStreams:
    """Named aggregated streams that are independently written to PostgreSQL."""

    requests: DataFrame
    endpoints: DataFrame
    ips: DataFrame


def parse_kafka_events(kafka_events: DataFrame) -> DataFrame:
    """Decode Kafka JSON values and discard malformed or incomplete records."""
    decoded = kafka_events.select(
        F.from_json(F.col("value").cast("string"), LOG_SCHEMA).alias("log")
    ).select("log.*")
    parsed = decoded.withColumn("event_time", F.to_timestamp("timestamp"))
    return parsed.filter(
        F.col("event_time").isNotNull()
        & F.col("ip_address").isNotNull()
        & F.col("endpoint").isNotNull()
        & F.col("status_code").isNotNull()
        & F.col("response_time_ms").isNotNull()
        & (F.col("response_time_ms") >= 0)
    )


def create_metric_streams(events: DataFrame) -> MetricStreams:
    """Create one-minute aggregate streams from validated events."""
    windowed = events.withWatermark("event_time", "2 minutes")
    error = F.when(F.col("status_code") >= 400, 1).otherwise(0)

    requests = (
        windowed.groupBy(F.window("event_time", "1 minute"))
        .agg(
            F.count("*").alias("total_requests"),
            F.sum(error).alias("total_errors"),
            F.avg("response_time_ms").alias("average_response_time_ms"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_requests",
            "total_errors",
            F.round(F.col("total_errors") * 100 / F.col("total_requests"), 2).alias(
                "error_rate_percent"
            ),
            F.round("average_response_time_ms", 2).alias("average_response_time_ms"),
        )
    )
    endpoints = (
        windowed.groupBy(F.window("event_time", "1 minute"), "endpoint")
        .agg(
            F.count("*").alias("total_requests"),
            F.sum(error).alias("total_errors"),
            F.avg("response_time_ms").alias("average_response_time_ms"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "endpoint",
            "total_requests",
            "total_errors",
            F.round("average_response_time_ms", 2).alias("average_response_time_ms"),
        )
    )
    ips = (
        windowed.groupBy(F.window("event_time", "1 minute"), "ip_address")
        .agg(F.count("*").alias("request_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "ip_address",
            "request_count",
        )
    )
    return MetricStreams(requests=requests, endpoints=endpoints, ips=ips)
