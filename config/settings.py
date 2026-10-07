"""Shared settings loaded from environment variables."""

from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "web_logs")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "log_analytics")
    postgres_user: str = os.getenv("POSTGRES_USER", "analytics_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "change_this_password")
    error_rate_threshold: float = float(os.getenv("ERROR_RATE_THRESHOLD", "10"))
    requests_per_ip_threshold: int = int(os.getenv("REQUESTS_PER_IP_THRESHOLD", "50"))
    slow_response_threshold_ms: float = float(os.getenv("SLOW_RESPONSE_THRESHOLD_MS", "1000"))


settings = Settings()
