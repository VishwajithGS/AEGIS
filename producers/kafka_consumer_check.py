"""Kafka consumer verification: read from the AEGIS topics with a consumer
group, parse JSON and report per-topic counts.

    python -m producers.kafka_consumer_check --timeout 20 --max 500
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError

from aegis.config import get_config
from aegis.logging_config import setup_logging

log = setup_logging("kafka-consumer-check")


def consume(cfg: dict, topics: list[str], group: str, timeout_s: float, max_messages: int,
            from_beginning: bool = True, bootstrap: str | None = None) -> dict:
    c = Consumer({
        "bootstrap.servers": bootstrap or cfg["kafka"]["bootstrap_servers"],
        "group.id": group,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": False,
    })
    c.subscribe(topics)
    counts, bad_json, sample = Counter(), Counter(), {}
    deadline = time.time() + timeout_s
    total = 0
    try:
        while time.time() < deadline and (not max_messages or total < max_messages):
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"consumer error: {msg.error()}")
                continue
            total += 1
            counts[msg.topic()] += 1
            try:
                obj = json.loads(msg.value())
                sample.setdefault(msg.topic(), {k: obj.get(k) for k in ("schema_version", "source_type")})
            except (ValueError, TypeError):
                bad_json[msg.topic()] += 1
        if total:
            c.commit(asynchronous=False)
    finally:
        c.close()
    return {"group": group, "consumed": total, "per_topic": dict(counts),
            "unparseable_json": dict(bad_json), "first_seen": sample}


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="Verify Kafka consumption of AEGIS topics")
    ap.add_argument("--topics", nargs="*", default=None)
    ap.add_argument("--group", default="aegis-verify")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--max", type=int, default=1000)
    ap.add_argument("--latest", action="store_true", help="start from latest offsets for a new group")
    ap.add_argument("--bootstrap", default=None)
    args = ap.parse_args()
    t = cfg["kafka"]["topics"]
    topics = args.topics or [t["weather"], t["water_level"], t["traffic"], t["incidents"]]
    print(json.dumps(consume(cfg, topics, args.group, args.timeout, args.max, not args.latest, args.bootstrap), indent=2))


if __name__ == "__main__":
    main()
