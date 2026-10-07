from producer.log_generator import LogGenerator
from producer.scenarios import Scenario


def test_normal_event_has_required_fields() -> None:
    event = LogGenerator(seed=7).generate()

    assert set(event) == {
        "timestamp",
        "ip_address",
        "method",
        "endpoint",
        "status_code",
        "response_time_ms",
        "user_agent",
    }
    assert event["status_code"] in {200, 201, 404, 500}
    assert event["response_time_ms"] > 0


def test_error_spike_event_is_a_server_error() -> None:
    event = LogGenerator(seed=7).generate(Scenario.ERROR_SPIKE)

    assert event["endpoint"] == "/api/orders"
    assert event["status_code"] == 500


def test_suspicious_ip_event_uses_fixed_demo_address() -> None:
    event = LogGenerator(seed=7).generate(Scenario.SUSPICIOUS_IP)

    assert event["ip_address"] == "203.0.113.42"


def test_slow_endpoint_event_exceeds_threshold() -> None:
    event = LogGenerator(seed=7).generate(Scenario.SLOW_ENDPOINT)

    assert event["endpoint"] == "/api/reports"
    assert event["response_time_ms"] > 1_000
