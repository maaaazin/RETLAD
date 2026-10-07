"""Traffic scenarios used to make the live demo repeatable."""

from enum import Enum


class Scenario(str, Enum):
    """Supported traffic patterns for generated log events."""

    NORMAL = "normal"
    ERROR_SPIKE = "error_spike"
    SUSPICIOUS_IP = "suspicious_ip"
    SLOW_ENDPOINT = "slow_endpoint"

