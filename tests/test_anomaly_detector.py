from datetime import datetime, timezone
import re
import subprocess

import pytest

from pyspark.sql import SparkSession

from spark.anomaly_detector import (
    detect_error_spikes,
    detect_slow_endpoints,
    detect_suspicious_ips,
)


def _java_major_version() -> int | None:
    result = subprocess.run(
        ["java", "-version"], capture_output=True, text=True, check=False
    )
    match = re.search(r'version "(\d+)', result.stderr)
    return int(match.group(1)) if match else None


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    java_major = _java_major_version()
    if java_major is not None and java_major > 21:
        pytest.skip(f"Spark local tests require a supported JDK; found Java {java_major}.")
    session = SparkSession.builder.master("local[1]").appName("retlad-tests").getOrCreate()
    yield session
    session.stop()


def test_error_spike_creates_alert(spark: SparkSession) -> None:
    metrics = spark.createDataFrame(
        [(datetime.now(timezone.utc), 12.5)], ["window_start", "error_rate_percent"]
    )

    alert = detect_error_spikes(metrics).first()

    assert alert.anomaly_type == "error_rate_spike"
    assert alert.severity == "warning"


def test_suspicious_ip_creates_alert(spark: SparkSession) -> None:
    metrics = spark.createDataFrame(
        [(datetime.now(timezone.utc), "203.0.113.42", 55)],
        ["window_start", "ip_address", "request_count"],
    )

    alert = detect_suspicious_ips(metrics).first()

    assert alert.anomaly_type == "suspicious_ip_traffic"
    assert alert.ip_address == "203.0.113.42"


def test_slow_endpoint_creates_alert(spark: SparkSession) -> None:
    metrics = spark.createDataFrame(
        [(datetime.now(timezone.utc), "/api/reports", 1100.0)],
        ["window_start", "endpoint", "average_response_time_ms"],
    )

    alert = detect_slow_endpoints(metrics).first()

    assert alert.anomaly_type == "slow_endpoint"
    assert alert.endpoint == "/api/reports"
