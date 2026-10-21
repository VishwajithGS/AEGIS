"""Canonical mapping, validation, timestamps, units and de-duplication (Spark)."""
from __future__ import annotations

import json
import random
from datetime import datetime, timezone

import pytest

from aegis.geo import ZoneGrid

pytestmark = pytest.mark.spark

NOW_MS = int(datetime.now(timezone.utc).timestamp() * 1000)


def raw_df(spark, rows):
    """rows: list of (topic, payload) where payload is a dict or a raw string."""
    data = [(p if isinstance(p, str) else json.dumps(p), t, None, None, None, "test", i) for i, (t, p) in enumerate(rows)]
    return spark.createDataFrame(data, "value string, topic string, kafka_partition int, kafka_offset long, "
                                       "ingest_time timestamp, origin string, source_seq long")


def canon(spark, cfg, rows):
    """Canonicalise and collect. ``event_time_utc`` is rendered by Spark (session time
    zone UTC) because collect() converts timestamps to the driver's local time zone."""
    from pyspark.sql import functions as F
    from spark_jobs import transforms as TR
    df = TR.union_canonical(TR.canonicalize_all(raw_df(spark, rows), cfg).values())
    df = df.withColumn("event_time_utc", F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss"))
    return {r["event_id"] or f"_{i}": r.asDict() for i, r in enumerate(df.collect())}


WX_METRIC = {"event_id": "WX-m1", "source_type": "weather", "source_id": "WXM-01", "schema_version": "wx-metric-1",
             "synthetic": True, "event_time": "2026-10-20T15:30:00.000+05:30", "lat": 13.05, "lon": 80.22,
             "temperature_c": 27.5, "humidity_pct": 91, "rainfall_mm_hr": 12.4, "condition": "Heavy Rain"}
WX_IMP = {"obs_id": "WX-i1", "source_type": "weather", "station": "AWS-11", "schema_version": "wx-imperial-2",
          "synthetic": True, "ts_ms": 1760954400000, "location": {"latitude": 13.0, "longitude": 80.15},
          "temp_f": 80.6, "rh_pct": 88, "precip_in_hr": 0.5, "sky": "+RA"}
WL = {"event_id": "WL-1", "source_type": "water_level", "source_id": "WLS-01", "schema_version": "wl-1",
      "synthetic": True, "event_time": "2026-10-20T10:00:00Z", "lat": 13.07, "lon": 80.2, "river_id": "RIVER-A",
      "water_level": 245.0, "unit": "cm", "danger_level": 3.5, "danger_level_unit": "m", "sensor_status": "FAULT"}
TR_EV = {"event_id": "TR-1", "source_type": "traffic", "source_id": "TRF-CAM-001", "schema_version": "tr-1",
         "synthetic": True, "event_time": "2026-10-20 15:30:00", "segment_id": "SEG-001", "lat": 13.0, "lon": 80.2,
         "vehicle_density": 40, "avg_speed_kmph": 12, "free_flow_speed_kmph": 50, "disruption": "congested"}
PR = {"event_id": "PR-1", "source_type": "public_report", "source_id": "citizen-app", "schema_version": "pr-1",
      "synthetic": True, "event_time": "2026-10-20T15:35:00+05:30", "report_id": "PR-1", "lat": None, "lon": None,
      "category": "Water logging", "description": "x", "channel": "app", "verification_status": "Confirmed",
      "confidence": 0.8}


def test_weather_formats_map_to_canonical_units_and_utc(spark, cfg):
    out = canon(spark, cfg, [("weather-events", WX_METRIC), ("weather-events", WX_IMP)])
    m, i = out["WX-m1"], out["WX-i1"]
    assert m["is_valid"] and i["is_valid"]
    assert m["event_time_utc"] == "2026-10-20T10:00:00"                # +05:30 -> UTC
    assert i["event_time_utc"] == "2025-10-20T10:00:00"                # epoch ms 1760954400000
    assert i["source_id"] == "AWS-11" and i["lat"] == 13.0              # nested location + renamed keys
    assert i["temperature_c"] == pytest.approx(27.0, abs=0.01)          # 80.6 F
    assert i["rainfall_mm_hr"] == pytest.approx(12.7, abs=1e-3)         # 0.5 in/hr
    assert m["condition"] == "heavy_rain" and i["condition"] == "heavy_rain"
    assert m["zone_id"] == ZoneGrid.from_config(cfg).zone_for(13.05, 80.22)


def test_water_level_units_ratio_and_fault_flag(spark, cfg):
    w = canon(spark, cfg, [("water-level-events", WL)])["WL-1"]
    assert w["is_valid"] and w["water_level_m"] == pytest.approx(2.45) and w["level_ratio"] == pytest.approx(0.7)
    assert w["sensor_status"] == "fault" and "sensor_fault" in w["dq_flags"]


def test_traffic_local_time_and_disruption(spark, cfg):
    t = canon(spark, cfg, [("traffic-events", TR_EV)])["TR-1"]
    assert t["event_time_utc"] == "2026-10-20T10:00:00"                # naive Asia/Kolkata -> UTC
    assert t["speed_ratio"] == pytest.approx(0.24) and t["disruption_level"] == "slow" and t["is_disrupted"]


def test_report_normalisation_and_missing_coordinates(spark, cfg):
    p = canon(spark, cfg, [("incident-reports", PR)])["PR-1"]
    assert p["is_valid"] and p["category"] == "waterlogging" and p["verification_status"] == "verified"
    assert p["zone_id"] is None and "missing_coordinates" in p["dq_flags"] and p["report_weight"] == 1.0
    assert p["severity"] is None                                        # reports carry no observed severity


@pytest.mark.parametrize("payload,topic,reason", [
    ('{"event_id": "x", "source_type": "weath', "weather-events", "malformed_json"),
    ({**WX_METRIC, "event_id": "a1", "rainfall_mm_hr": "heavy"}, "weather-events", "invalid_type:rainfall"),
    ({k: v for k, v in WX_METRIC.items() if k != "event_time"} | {"event_id": "a2"}, "weather-events", "missing_required:event_time"),
    ({**WX_METRIC, "event_id": "a3", "humidity_pct": 140}, "weather-events", "out_of_range:humidity_pct"),
    ({**WX_METRIC, "event_id": "a4", "lat": 95.5}, "weather-events", "invalid_coordinates"),
    ({**WX_METRIC, "event_id": "a5", "lat": 14.2}, "weather-events", "outside_study_region"),
    ({**WX_METRIC, "event_id": "a6", "schema_version": "wx-metric-0"}, "weather-events", "unknown_schema_version"),
    ({**WX_METRIC, "event_id": "a7", "event_time": "not a time"}, "weather-events", "invalid_timestamp"),
    ({**WX_METRIC, "event_id": "a8", "event_time": "2099-01-01T00:00:00Z"}, "weather-events", "future_timestamp"),
    ({**WL, "event_id": "a9", "unit": "furlong"}, "water-level-events", "invalid_unit"),
    ({**WL, "event_id": "a10", "water_level": 9999}, "water-level-events", "out_of_range:water_level_m"),
    ({**WX_IMP, "obs_id": "a11", "precip_in_hr": 15}, "weather-events", "out_of_range:rainfall_mm_hr"),
    ({**PR, "event_id": "a12", "category": None}, "incident-reports", "missing_required:category"),
    ({**PR, "event_id": "a13", "confidence": 1.7}, "incident-reports", "out_of_range:confidence"),
    ({**WL, "event_id": "a14"}, "weather-events", "topic_source_mismatch"),
])
def test_rejections_carry_reasons(spark, cfg, payload, topic, reason):
    rows = list(canon(spark, cfg, [(topic, payload)]).values())
    assert len(rows) == 1 and rows[0]["is_valid"] is False
    assert reason in rows[0]["reasons"], rows[0]["reasons"]


def test_late_arrival_flag(spark, cfg):
    from spark_jobs import transforms as TR
    ev = {**WX_METRIC, "event_id": "late-1", "event_time": "2026-10-20T10:00:00Z"}
    df = spark.createDataFrame([(json.dumps(ev), "weather-events", 0, 1,
                                datetime(2026, 10, 20, 10, 30, tzinfo=timezone.utc), "kafka", 1)],
                               "value string, topic string, kafka_partition int, kafka_offset long, "
                               "ingest_time timestamp, origin string, source_seq long")
    row = TR.canonicalize(df, "weather", cfg).first()
    assert row["is_valid"] and "late_arrival" in row["dq_flags"]          # 30 min behind > 10 min watermark


def test_zone_id_parity_with_python(spark, cfg):
    from pyspark.sql import functions as F
    from spark_jobs.transforms import zone_id_col
    grid = ZoneGrid.from_config(cfg)
    rng = random.Random(5)
    pts = [(rng.uniform(12.8, 13.3), rng.uniform(80.05, 80.4)) for _ in range(400)]
    pts += [(12.85 + 0.05 * k, 80.10 + 0.05 * (k % 6)) for k in range(9)]           # exact cell boundaries
    df = spark.createDataFrame(pts, "lat double, lon double").withColumn("z", zone_id_col(F.col("lat"), F.col("lon"), grid))
    for r in df.collect():
        assert r["z"] == grid.zone_for(r["lat"], r["lon"]), (r["lat"], r["lon"])


def test_haversine_column_matches_python(spark):
    from pyspark.sql import functions as F
    from aegis.geo import haversine_km
    from spark_jobs.transforms import haversine_km_col
    pts = [(13.0827, 80.2707, 12.9941, 80.1709), (0.0, 0.0, 0.0, 1.0), (13.0, 80.0, 13.0, 80.0)]
    df = spark.createDataFrame(pts, "a double, b double, c double, d double")
    got = df.select(haversine_km_col(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km")).collect()
    for (a, b, c, d), r in zip(pts, got):
        assert r["km"] == pytest.approx(haversine_km(a, b, c, d), abs=1e-9)


def test_dedup_keeps_first_arrival(spark, cfg):
    from pyspark.sql import functions as F
    from spark_jobs import transforms as TR
    rows = [("weather-events", WX_METRIC), ("weather-events", WX_METRIC), ("weather-events", {**WX_METRIC, "event_id": "other"})]
    df = TR.union_canonical(TR.canonicalize_all(raw_df(spark, rows), cfg).values()).filter(F.col("is_valid"))
    d = TR.dedup_batch(df)
    assert df.count() == 3 and d.count() == 2
    assert d.filter(F.col("event_id") == "WX-m1").first()["source_seq"] == 0
