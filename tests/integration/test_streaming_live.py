"""Kafka -> Structured Streaming -> Parquet/HBase/Kafka, on isolated topics.

Runs the real streaming job as a subprocess with ``--available-now`` against
uniquely named topics and a temporary data root, so it cannot touch demo data.
Requires Kafka, HBase and a Spark runtime (run inside the aegis-spark container).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]


def _iso_now(offset_s: float = 0.0) -> str:
    return datetime.fromtimestamp(time.time() + offset_s, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def test_streaming_end_to_end(require_kafka, require_hbase, spark, tmp_path):
    from aegis.config import load_config
    from aegis.storage.hbase_repository import HBaseRepository, sensor_row_key
    from producers.kafka_admin import create_topics, describe_topics
    from producers.formats import local_naive
    from producers.kafka_client import AegisProducer, admin_client

    run = uuid.uuid4().hex[:6]
    names = {"weather": f"it-{run}-weather", "water_level": f"it-{run}-water", "traffic": f"it-{run}-traffic",
             "incidents": f"it-{run}-incidents", "zone_risk": f"it-{run}-risk", "data_quality": f"it-{run}-dq"}
    env = {**os.environ, "AEGIS__STORAGE__DATA_ROOT": str(tmp_path / "data"), "AEGIS_LOG_LEVEL": "WARNING"}
    env.update({f"AEGIS__KAFKA__TOPICS__{k.upper()}": v for k, v in names.items()})
    cfg = load_config(environ=env)
    create_topics(cfg)
    try:
        p = AegisProducer(cfg, client_id="aegis-it-stream")
        base = {"source_type": "weather", "schema_version": "wx-metric-1", "synthetic": True, "lat": 13.05, "lon": 80.22,
                "temperature_c": 27, "humidity_pct": 90, "condition": "Rain"}
        events = [{**base, "event_id": f"{run}-w{i}", "source_id": "WXM-IT", "event_time": _iso_now(-i), "rainfall_mm_hr": 20.0 + i}
                  for i in range(3)]
        for e in events:
            p.send(names["weather"], "WXM-IT", json.dumps(e))
        p.send(names["weather"], "WXM-IT", json.dumps(events[0]))                      # duplicate
        p.send(names["weather"], "WXM-IT", '{"event_id": "broken", "source_type": ')    # malformed
        p.send(names["water_level"], "WLS-IT", json.dumps({
            "event_id": f"{run}-l1", "source_type": "water_level", "source_id": "WLS-IT", "schema_version": "wl-1",
            "synthetic": True, "event_time": _iso_now(), "lat": 13.05, "lon": 80.22, "water_level": 9999, "unit": "m"}))
        p.send(names["traffic"], "TRF-IT", json.dumps({
            "event_id": f"{run}-t1", "source_type": "traffic", "source_id": "TRF-IT", "schema_version": "tr-1",
            "synthetic": True, "event_time": local_naive(time.time()),      # Asia/Kolkata wall time, no offset
            "segment_id": "SEG-IT", "lat": 13.05, "lon": 80.22, "vehicle_density": 30, "avg_speed_kmph": 10,
            "free_flow_speed_kmph": 50, "disruption": "BLOCKED"}))
        assert p.flush(30) == 0

        proc = subprocess.run([sys.executable, "-m", "spark_jobs.streaming_pipeline", "--available-now",
                               "--starting-offsets", "earliest", "--require-hbase"],
                              env=env, capture_output=True, text=True, timeout=600)
        assert proc.returncode == 0, proc.stdout[-3000:] + proc.stderr[-3000:]

        silver = spark.read.parquet(str(tmp_path / "data" / "lake" / "silver_stream" / "weather"))
        assert sorted(r["event_id"] for r in silver.collect()) == sorted(e["event_id"] for e in events)   # dedup
        q = spark.read.parquet(str(tmp_path / "data" / "lake" / "quarantine" / "stream"))
        codes = {c for r in q.collect() for c in r["reason_codes"]}
        assert {"malformed_json", "out_of_range"} <= codes
        bronze = spark.read.parquet(str(tmp_path / "data" / "lake" / "bronze" / "kafka_raw"))
        assert bronze.count() == 7
        risk = spark.read.parquet(str(tmp_path / "data" / "lake" / "live" / "zone_risk"))
        assert risk.count() > 0 and risk.filter("risk_score < 0 OR risk_score > 100").count() == 0

        with HBaseRepository.from_config(cfg) as repo:
            e = events[1]
            ts = int(datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")).timestamp() * 1000)
            row = repo.get("sensor_observations", sensor_row_key("WXM-IT", ts, e["event_id"]))
            assert row and row["metadata"]["event_id"] == e["event_id"] and row["measurement"]["rainfall_mm_hr"] == e["rainfall_mm_hr"]
            assert repo.latest_with_prefix("zone_risk_history", f"{risk.first()['zone_id']}#") is not None
        offsets = describe_topics(cfg)
        assert offsets[names["zone_risk"]]["messages"] > 0 and offsets[names["data_quality"]]["messages"] > 0
    finally:
        admin_client(cfg).delete_topics(list(names.values()))
