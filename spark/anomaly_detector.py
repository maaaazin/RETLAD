"""Rule-based anomaly detection for the streaming aggregate tables."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import settings


def detect_error_spikes(request_metrics: DataFrame) -> DataFrame:
    """Alert when the one-minute HTTP error rate is too high."""
    return request_metrics.filter(F.col("error_rate_percent") > settings.error_rate_threshold).select(
        "window_start",
        F.lit("error_rate_spike").alias("anomaly_type"),
        F.when(F.col("error_rate_percent") > settings.error_rate_threshold * 2, "critical")
        .otherwise("warning")
        .alias("severity"),
        F.lit(None).cast("string").alias("ip_address"),
        F.lit(None).cast("string").alias("endpoint"),
        F.col("error_rate_percent").alias("metric_value"),
        F.lit(settings.error_rate_threshold).alias("threshold_value"),
        F.concat(
            F.lit("Error rate reached "),
            F.col("error_rate_percent").cast("string"),
            F.lit("% in a one-minute window."),
        ).alias("description"),
    )


def detect_suspicious_ips(ip_metrics: DataFrame) -> DataFrame:
    """Alert when a single IP sends excessive requests in one minute."""
    return ip_metrics.filter(F.col("request_count") > settings.requests_per_ip_threshold).select(
        "window_start",
        F.lit("suspicious_ip_traffic").alias("anomaly_type"),
        F.when(F.col("request_count") > settings.requests_per_ip_threshold * 2, "critical")
        .otherwise("warning")
        .alias("severity"),
        "ip_address",
        F.lit(None).cast("string").alias("endpoint"),
        F.col("request_count").cast("double").alias("metric_value"),
        F.lit(settings.requests_per_ip_threshold).cast("double").alias("threshold_value"),
        F.concat(
            F.lit("IP "),
            F.col("ip_address"),
            F.lit(" sent "),
            F.col("request_count").cast("string"),
            F.lit(" requests in a one-minute window."),
        ).alias("description"),
    )


def detect_slow_endpoints(endpoint_metrics: DataFrame) -> DataFrame:
    """Alert when an endpoint has a high one-minute mean response time."""
    return endpoint_metrics.filter(
        F.col("average_response_time_ms") > settings.slow_response_threshold_ms
    ).select(
        "window_start",
        F.lit("slow_endpoint").alias("anomaly_type"),
        F.when(
            F.col("average_response_time_ms") > settings.slow_response_threshold_ms * 2,
            "critical",
        )
        .otherwise("warning")
        .alias("severity"),
        F.lit(None).cast("string").alias("ip_address"),
        "endpoint",
        F.col("average_response_time_ms").alias("metric_value"),
        F.lit(settings.slow_response_threshold_ms).alias("threshold_value"),
        F.concat(
            F.lit("Endpoint "),
            F.col("endpoint"),
            F.lit(" averaged "),
            F.col("average_response_time_ms").cast("string"),
            F.lit(" ms in a one-minute window."),
        ).alias("description"),
    )
