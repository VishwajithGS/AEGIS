"""Kafka producer wrapper with delivery tracking and a dead-letter file.

Delivery guarantee implemented here: **at-least-once** from producer to
broker. ``acks=all`` + ``enable.idempotence=true`` stop broker-side
duplicates caused by producer retries. A record whose delivery finally fails
(after librdkafka's retries and ``delivery.timeout.ms``) is appended to
``data/dead_letter/producer_failures.jsonl`` with the error, so it is never
lost silently and can be replayed with ``python -m producers.replay_dead_letter``.
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Optional

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient

from aegis.config import data_path
from aegis.logging_config import setup_logging

log = setup_logging("kafka")


def producer_conf(cfg: dict, bootstrap: Optional[str] = None, client_id: str = "aegis-producer") -> dict:
    p = cfg["kafka"]["producer"]
    return {
        "bootstrap.servers": bootstrap or cfg["kafka"]["bootstrap_servers"],
        "client.id": client_id,
        "acks": str(p["acks"]),
        "enable.idempotence": bool(p["enable_idempotence"]),
        "retries": int(p["retries"]),
        "retry.backoff.ms": int(p["retry_backoff_ms"]),
        "delivery.timeout.ms": int(p["delivery_timeout_ms"]),
        "linger.ms": int(p["linger_ms"]),
        "compression.type": str(p["compression_type"]),
    }


class AegisProducer:
    def __init__(self, cfg: dict, bootstrap: Optional[str] = None, client_id: str = "aegis-producer"):
        self.cfg = cfg
        self.conf = producer_conf(cfg, bootstrap, client_id)
        self._producer = Producer(self.conf)
        self.stats: Counter = Counter()
        self._lock = threading.Lock()
        self.dead_letter = data_path(cfg, "dead_letter_dir", "producer_failures.jsonl")

    def check_connection(self, timeout: float = 10.0) -> list[str]:
        """Return broker topic names; raises KafkaException if unreachable."""
        md = self._producer.list_topics(timeout=timeout)
        return sorted(md.topics)

    def _on_delivery(self, err: Optional[KafkaError], msg) -> None:
        with self._lock:
            if err is None:
                self.stats["delivered"] += 1
                self.stats[f"delivered.{msg.topic()}"] += 1
                return
            self.stats["failed"] += 1
        log.error(f"delivery failed topic={msg.topic()} error={err}")
        self._write_dead_letter(msg.topic(), msg.key(), msg.value(), str(err))

    def _write_dead_letter(self, topic: str, key: Optional[bytes], value: Optional[bytes], error: str) -> None:
        self.dead_letter.parent.mkdir(parents=True, exist_ok=True)
        rec = {"failed_at": time.time(), "topic": topic, "error": error,
               "key": key.decode("utf-8", "replace") if key else None,
               "value": value.decode("utf-8", "replace") if value else None}
        with self._lock, self.dead_letter.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")

    def send(self, topic: str, key: str, value: str) -> None:
        payload = value.encode("utf-8")
        while True:
            try:
                self._producer.produce(topic, key=key.encode("utf-8"), value=payload, on_delivery=self._on_delivery)
                self.stats["sent"] += 1
                break
            except BufferError:
                # Local queue full: serve delivery callbacks (backpressure) and retry.
                self.stats["backpressure"] += 1
                self._producer.poll(0.5)
            except KafkaException as exc:
                self.stats["failed"] += 1
                log.error(f"produce rejected topic={topic} error={exc}")
                self._write_dead_letter(topic, key.encode(), payload, str(exc))
                break
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning(f"{remaining} messages still undelivered after flush({timeout}s)")
        return remaining


def admin_client(cfg: dict, bootstrap: Optional[str] = None) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap or cfg["kafka"]["bootstrap_servers"]})
