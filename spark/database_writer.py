"""PostgreSQL writers called by Spark's foreachBatch sinks."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

from config.settings import settings

SPARK_LOCAL_TIMEZONE = timezone(timedelta(hours=5, minutes=30))


def _timestamp_as_utc(value: datetime) -> datetime:
    """Convert Spark's timezone-naive Python timestamps to UTC for PostgreSQL."""
    if value.tzinfo is None:
        return value.replace(tzinfo=SPARK_LOCAL_TIMEZONE).astimezone(timezone.utc)
    return value.astimezone(timezone.utc)


def _connection() -> psycopg.Connection[Any]:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def _execute_batch(sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    rows = list(rows)
    if not rows:
        return
    with _connection() as connection, connection.cursor() as cursor:
        cursor.executemany(sql, rows)


def write_request_metrics(batch_df: Any, _: int) -> None:
    rows = (
        (_timestamp_as_utc(row.window_start), _timestamp_as_utc(row.window_end), row.total_requests, row.total_errors,
         row.error_rate_percent, row.average_response_time_ms)
        for row in batch_df.collect()
    )
    _execute_batch(
        """
        INSERT INTO request_metrics
          (window_start, window_end, total_requests, total_errors, error_rate_percent, average_response_time_ms)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start) DO UPDATE SET
          window_end = EXCLUDED.window_end,
          total_requests = EXCLUDED.total_requests,
          total_errors = EXCLUDED.total_errors,
          error_rate_percent = EXCLUDED.error_rate_percent,
          average_response_time_ms = EXCLUDED.average_response_time_ms
        """,
        rows,
    )


def write_endpoint_metrics(batch_df: Any, _: int) -> None:
    rows = (
        (_timestamp_as_utc(row.window_start), _timestamp_as_utc(row.window_end), row.endpoint, row.total_requests,
         row.total_errors, row.average_response_time_ms)
        for row in batch_df.collect()
    )
    _execute_batch(
        """
        INSERT INTO endpoint_metrics
          (window_start, window_end, endpoint, total_requests, total_errors, average_response_time_ms)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (window_start, endpoint) DO UPDATE SET
          window_end = EXCLUDED.window_end,
          total_requests = EXCLUDED.total_requests,
          total_errors = EXCLUDED.total_errors,
          average_response_time_ms = EXCLUDED.average_response_time_ms
        """,
        rows,
    )


def write_ip_metrics(batch_df: Any, _: int) -> None:
    rows = (
        (_timestamp_as_utc(row.window_start), _timestamp_as_utc(row.window_end), row.ip_address, row.request_count)
        for row in batch_df.collect()
    )
    _execute_batch(
        """
        INSERT INTO ip_metrics (window_start, window_end, ip_address, request_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (window_start, ip_address) DO UPDATE SET
          window_end = EXCLUDED.window_end,
          request_count = EXCLUDED.request_count
        """,
        rows,
    )


def write_anomalies(batch_df: Any, _: int) -> None:
    rows = (
        (_timestamp_as_utc(row.window_start), row.anomaly_type, row.severity, row.ip_address, row.endpoint,
         row.metric_value, row.threshold_value, row.description)
        for row in batch_df.collect()
    )
    _execute_batch(
        """
        INSERT INTO anomalies
          (window_start, anomaly_type, severity, ip_address, endpoint, metric_value, threshold_value, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
