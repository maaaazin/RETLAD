"""Generate realistic, non-sensitive web-server log events for the demo."""

from __future__ import annotations

from datetime import datetime, timezone
import random
from typing import Any

from faker import Faker

from producer.scenarios import Scenario


class LogGenerator:
    """Create normal and intentionally anomalous web request events."""

    endpoints = (
        "/",
        "/products",
        "/products/search",
        "/cart",
        "/checkout",
        "/api/products",
        "/api/orders",
        "/api/users/profile",
        "/api/reports",
    )
    methods = ("GET", "GET", "GET", "POST", "PUT")
    suspicious_ip = "203.0.113.42"  # Reserved documentation-only address.
    slow_endpoint = "/api/reports"
    error_endpoint = "/api/orders"

    def __init__(self, seed: int | None = None) -> None:
        self._random = random.Random(seed)
        self._faker = Faker()
        if seed is not None:
            self._faker.seed_instance(seed)

    def generate(self, scenario: Scenario = Scenario.NORMAL) -> dict[str, Any]:
        """Return one JSON-serializable event for the requested scenario."""
        event = self._normal_event()

        if scenario is Scenario.ERROR_SPIKE:
            event.update(
                endpoint=self.error_endpoint,
                status_code=500,
                response_time_ms=self._random.randint(650, 1_400),
            )
        elif scenario is Scenario.SUSPICIOUS_IP:
            event.update(
                ip_address=self.suspicious_ip,
                endpoint="/api/users/profile",
                status_code=self._random.choices((200, 401), weights=(75, 25), k=1)[0],
            )
        elif scenario is Scenario.SLOW_ENDPOINT:
            event.update(
                endpoint=self.slow_endpoint,
                status_code=200,
                response_time_ms=self._random.randint(1_100, 2_500),
            )

        # This label is for explaining a demonstration. Spark detects anomalies
        # from the actual request behaviour, not from this field.
        event["traffic_type"] = scenario.value
        return event

    def _normal_event(self) -> dict[str, Any]:
        status_code = self._random.choices(
            population=(200, 201, 404, 500), weights=(82, 8, 7, 3), k=1
        )[0]
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "ip_address": self._faker.ipv4_public(),
            "method": self._random.choice(self.methods),
            "endpoint": self._random.choice(self.endpoints),
            "status_code": status_code,
            "response_time_ms": self._response_time(status_code),
            "user_agent": self._faker.user_agent(),
        }

    def _response_time(self, status_code: int) -> int:
        if status_code >= 500:
            return self._random.randint(400, 1_200)
        if status_code >= 400:
            return self._random.randint(80, 450)
        return self._random.randint(25, 500)
