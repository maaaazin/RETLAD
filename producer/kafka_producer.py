"""Send simulated web-server logs to the Kafka ingestion topic."""

from __future__ import annotations

import argparse
import json
import logging
import time

from confluent_kafka import Producer

from config.settings import settings
from producer.log_generator import LogGenerator
from producer.scenarios import Scenario

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream simulated web logs to Kafka.")
    parser.add_argument(
        "--scenario",
        choices=[scenario.value for scenario in Scenario],
        default=Scenario.NORMAL.value,
        help="Traffic pattern to generate.",
    )
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=5.0,
        help="Number of generated events per second.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after this many events; omit to stream until interrupted.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional repeatable random seed.")
    return parser


def delivery_report(error: Exception | None, message: object) -> None:
    if error is not None:
        logger.error("Kafka delivery failed: %s", error)


def main() -> None:
    args = build_parser().parse_args()
    if args.events_per_second <= 0:
        raise SystemExit("--events-per-second must be greater than zero.")
    if args.max_events is not None and args.max_events <= 0:
        raise SystemExit("--max-events must be greater than zero.")

    scenario = Scenario(args.scenario)
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    generator = LogGenerator(seed=args.seed)
    delay_seconds = 1 / args.events_per_second
    sent_events = 0

    logger.info(
        "Streaming %s traffic to topic %s at %.2f events/second.",
        scenario.value,
        settings.kafka_topic,
        args.events_per_second,
    )
    try:
        while args.max_events is None or sent_events < args.max_events:
            event = generator.generate(scenario)
            producer.produce(
                settings.kafka_topic,
                key=event["ip_address"],
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            sent_events += 1
            time.sleep(delay_seconds)
    except KeyboardInterrupt:
        logger.info("Stopping log producer after %s events.", sent_events)
    finally:
        pending = producer.flush(10)
        if pending:
            logger.warning("%s message(s) were not delivered before shutdown.", pending)


if __name__ == "__main__":
    main()
