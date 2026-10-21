"""Kafka topic administration and verification.

    python -m producers.kafka_admin create      # create all AEGIS topics (idempotent)
    python -m producers.kafka_admin describe    # partitions and offsets per topic
"""
from __future__ import annotations

import argparse
import json
from typing import Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import NewTopic

from aegis.config import get_config
from aegis.logging_config import setup_logging
from producers.kafka_client import admin_client

log = setup_logging("kafka-admin")

RETENTION_MS = {  # source topics keep 7 days; derived topics 2 days
    "weather": 7 * 86400000, "water_level": 7 * 86400000, "traffic": 7 * 86400000, "incidents": 7 * 86400000,
    "zone_risk": 2 * 86400000, "data_quality": 2 * 86400000,
}


def create_topics(cfg: dict, bootstrap: Optional[str] = None, timeout: float = 30.0) -> dict:
    admin = admin_client(cfg, bootstrap)
    existing = set(admin.list_topics(timeout=timeout).topics)
    k = cfg["kafka"]
    wanted = []
    for name, topic in k["topics"].items():
        if topic in existing:
            continue
        wanted.append(NewTopic(topic, num_partitions=int(k["partitions"]),
                               replication_factor=int(k["replication_factor"]),
                               config={"retention.ms": str(RETENTION_MS.get(name, 7 * 86400000)),
                                       "cleanup.policy": "delete"}))
    result = {t: "exists" for t in k["topics"].values() if t in existing}
    if wanted:
        for topic, fut in admin.create_topics(wanted, operation_timeout=timeout).items():
            try:
                fut.result()
                result[topic] = "created"
                log.info(f"created topic {topic}")
            except KafkaException as exc:
                err = exc.args[0]
                if err.code() == err.TOPIC_ALREADY_EXISTS:
                    result[topic] = "exists"
                else:
                    result[topic] = f"error: {err}"
                    log.error(f"failed to create topic {topic}: {err}")
    return result


def describe_topics(cfg: dict, bootstrap: Optional[str] = None, timeout: float = 15.0) -> dict:
    """Partition count plus low/high watermark offsets (= retained message counts)."""
    bs = bootstrap or cfg["kafka"]["bootstrap_servers"]
    consumer = Consumer({"bootstrap.servers": bs, "group.id": "aegis-admin-describe",
                         "enable.auto.commit": False})
    try:
        md = consumer.list_topics(timeout=timeout)
        out = {}
        for name, topic in cfg["kafka"]["topics"].items():
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                out[topic] = {"exists": False}
                continue
            parts, total = [], 0
            for pid in sorted(t.partitions):
                lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=timeout)
                parts.append({"partition": pid, "low": lo, "high": hi})
                total += max(0, hi - lo)
            out[topic] = {"exists": True, "partitions": len(parts), "messages": total, "offsets": parts}
        return out
    finally:
        consumer.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="AEGIS Kafka topic administration")
    ap.add_argument("action", choices=["create", "describe"])
    ap.add_argument("--bootstrap", default=None)
    args = ap.parse_args()
    cfg = get_config()
    result = create_topics(cfg, args.bootstrap) if args.action == "create" else describe_topics(cfg, args.bootstrap)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
