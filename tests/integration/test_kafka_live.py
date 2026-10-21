"""Live Kafka round trip: create a temporary topic, produce, consume, delete."""
from __future__ import annotations

import json
import uuid

import pytest

pytestmark = pytest.mark.integration


def test_produce_and_consume_round_trip(require_kafka):
    from confluent_kafka.admin import NewTopic
    from producers.kafka_client import AegisProducer, admin_client
    from producers.kafka_consumer_check import consume
    cfg = require_kafka
    topic = f"aegis-it-{uuid.uuid4().hex[:8]}"
    admin = admin_client(cfg)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[topic].result(timeout=30)
    try:
        p = AegisProducer(cfg, client_id="aegis-it")
        ids = [f"it-{i}" for i in range(25)]
        for i in ids:
            p.send(topic, "it", json.dumps({"event_id": i, "synthetic": True}))
        assert p.flush(30) == 0
        assert p.stats["delivered"] == 25 and p.stats["failed"] == 0
        res = consume(cfg, [topic], f"aegis-it-{uuid.uuid4().hex[:6]}", timeout_s=30, max_messages=25)
        assert res["consumed"] == 25 and res["per_topic"][topic] == 25 and not res["unparseable_json"]
    finally:
        admin.delete_topics([topic])


def test_aegis_topics_exist(require_kafka):
    from producers.kafka_admin import create_topics, describe_topics
    cfg = require_kafka
    create_topics(cfg)
    d = describe_topics(cfg)
    for t in cfg["kafka"]["topics"].values():
        assert d[t]["exists"] and d[t]["partitions"] == cfg["kafka"]["partitions"]
