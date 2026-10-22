"""Real-data records through the existing Spark pipeline: canonical mapping,
provenance, risk without report/incident sources, ETL in real-historical mode,
real-data layers. Uses real excerpts from tests/fixtures/real (no network)."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

pytestmark = pytest.mark.spark
FIX = Path(__file__).resolve().parent.parent / "fixtures" / "real"
ERA5 = FIX / "era5_weather_2zones_2023-12-02_05.jsonl"


def raw_df(spark, lines, topic="weather-events"):
    data = [(l, topic, None, None, None, "file:test", i) for i, l in enumerate(lines)]
    return spark.createDataFrame(data, "value string, topic string, kafka_partition int, kafka_offset long, "
                                       "ingest_time timestamp, origin string, source_seq long")


def test_era5_records_map_to_canonical_weather(spark, cfg):
    from pyspark.sql import functions as F
    from spark_jobs import transforms as TR
    lines = ERA5.read_text(encoding="utf-8").splitlines()
    df = TR.canonicalize(raw_df(spark, lines), "weather", cfg)
    rows = df.select("event_id", "is_valid", "is_synthetic", "data_source", "observation_type", "zone_id", "source_id",
                     "rainfall_mm_hr", "condition", F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss").alias("t")).collect()
    assert len(rows) == 192 and all(r["is_valid"] for r in rows)
    assert {r["is_synthetic"] for r in rows} == {False}
    assert {r["data_source"] for r in rows} == {"era5-open-meteo"} and {r["observation_type"] for r in rows} == {"reanalysis"}
    assert all(r["zone_id"] == r["source_id"].replace("ERA5-", "") for r in rows)      # zone centre -> exact zone
    assert {r["condition"] for r in rows} == {"unknown"}                               # not invented
    src = {json.loads(l)["event_id"]: json.loads(l) for l in lines}
    for r in rows[:20]:
        assert r["rainfall_mm_hr"] == pytest.approx(src[r["event_id"]]["rainfall_mm_hr"])
        assert r["t"] + "Z" == src[r["event_id"]]["event_time"]


def test_synthetic_records_are_tagged_as_simulator(spark, cfg):
    from spark_jobs import transforms as TR
    ev = {"event_id": "WX-1", "source_type": "weather", "source_id": "WXM-01", "schema_version": "wx-metric-1",
          "synthetic": True, "event_time": "2026-10-20T15:30:00+05:30", "lat": 13.05, "lon": 80.22,
          "temperature_c": 27.0, "humidity_pct": 90, "rainfall_mm_hr": 5.0, "condition": "Rain"}
    r = TR.canonicalize(raw_df(spark, [json.dumps(ev)]), "weather", cfg).first()
    assert r["is_synthetic"] is True and r["data_source"] == "aegis-simulator" and r["observation_type"] == "synthetic"


def test_missing_report_source_is_not_observed(spark, cfg):
    from spark_jobs import transforms as TR
    from spark_jobs.risk_analytics import aggregate_signals, score, zone_signals
    canon = TR.with_all_domain_columns(
        TR.canonicalize(raw_df(spark, ERA5.read_text(encoding="utf-8").splitlines()), "weather", cfg))
    not_obs = score(aggregate_signals(zone_signals(canon, None, None), "1 hour", reports_observed=False), cfg).collect()
    assert all(r["weighted_reports"] is None for r in not_obs)
    assert {r["risk_category"] for r in not_obs} == {"Insufficient data"}             # rainfall only = 30 % weight
    assert all(r["risk_score"] is None and "incidents" in r["missing_features"] for r in not_obs)
    as_zero = score(aggregate_signals(zone_signals(canon, None, None), "1 hour", reports_observed=True), cfg).collect()
    assert all(r["weighted_reports"] == 0.0 for r in as_zero)                          # synthetic-mode behaviour unchanged


def test_hazard_index_is_null_without_incident_archive(spark):
    from spark_jobs.risk_analytics import hazard_index
    df = spark.createDataFrame([("Z-A", 0), ("Z-B", 0)], "zone_id string, archived_flood_incidents long")
    assert {r["historical_hazard_index"] for r in hazard_index(df).collect()} == {None}
    df2 = spark.createDataFrame([("Z-A", 2), ("Z-B", 4)], "zone_id string, archived_flood_incidents long")
    assert {r["zone_id"]: r["historical_hazard_index"] for r in hazard_index(df2).collect()} == {"Z-A": 0.5, "Z-B": 1.0}


def test_etl_and_real_layers_in_real_mode(spark, tmp_path):
    from aegis.config import load_config
    from spark_jobs.common import lake
    from spark_jobs.etl_pipeline import run
    from spark_jobs.real_data_layers import build
    cfg = load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(tmp_path / "data_real"),
                               "AEGIS__RISK__REPORTS_OBSERVED": "false", "AEGIS__HBASE__ENABLED": "false"})
    inp = tmp_path / "data_real" / "raw" / "historical"
    inp.mkdir(parents=True)
    shutil.copy(ERA5, inp / "weather.jsonl")
    shutil.copy(FIX / "rainfall_daily_2zones_2023-12-02_05.jsonl", inp / "rainfall_daily.jsonl")
    shutil.copy(FIX / "official_events_2023-11_12.jsonl", inp / "official_events.jsonl")
    (inp / "manifest.json").write_text(json.dumps({"data_mode": "real-historical", "synthetic": False}), encoding="utf-8")

    rep = run(spark, cfg, inp, include_stream=False, write_hbase=False)
    assert rep["data_mode"] == "real-historical"
    assert not [c for c in rep["reconciliation"] if c["passed"] is False], rep["reconciliation"]
    na = {c["check"] for c in rep["reconciliation"] if c["passed"] is None}
    assert any("injected" in c for c in na) and any("ML training" in c for c in na)
    assert rep["totals"]["records"] == 192 and rep["totals"]["rejected"] == 0
    for d in ("water_level", "traffic", "incidents"):                                # absent domains stay readable
        assert spark.read.parquet(lake(cfg, "silver", d)).count() == 0
    assert rep["gold"]["risk_category_distribution"] == {"Insufficient data": 192}

    v = build(spark, cfg, inp)
    assert v["era5_vs_imd_zone_days"]["pairs"] == 8 and v["era5_vs_imd_zone_days_alt_window"]["pairs"] >= 6
    assert v["gauge_vs_imd_cell"]["pairs"] == 4
    assert v["rows"]["official_events"] == 26
    z = spark.read.parquet(lake(cfg, "gold", "real_daily_rainfall"))
    assert z.count() == 8 and z.filter("imd_mm IS NULL").count() == 0
    dec4 = z.filter("zone_id = 'Z-R03-C01' AND date = '2023-12-04'").first()
    assert dec4["imd_category"] in ("very_heavy", "extremely_heavy") and dec4["era5_hours"] == 24


def test_etl_in_hybrid_mode_scores_zones_and_keeps_provenance(spark, tmp_path):
    """Real ERA5 + synthetic water/traffic/reports/incidents driven by it: zones get rule-based
    scores instead of 'Insufficient data', and every silver row says where it came from."""
    from aegis.config import load_config
    from producers.hybrid_generator import HYBRID_SOURCE, generate_hybrid
    from spark_jobs.common import lake
    from spark_jobs.etl_pipeline import run
    real = tmp_path / "data_real"
    hist = real / "raw" / "historical"
    hist.mkdir(parents=True)
    shutil.copy(ERA5, hist / "weather.jsonl")
    (hist / "manifest.json").write_text(json.dumps({"period": {"start": "2023-12-02", "end": "2023-12-05"}}),
                                        encoding="utf-8")
    cfg = load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(tmp_path / "data_hybrid"),
                               "AEGIS__HBASE__ENABLED": "false"})
    generate_hybrid(cfg, real, tmp_path / "data_hybrid", seed=42, step_minutes=30)
    rep = run(spark, cfg, tmp_path / "data_hybrid" / "raw" / "historical", include_stream=False, write_hbase=False)
    assert rep["data_mode"] == "hybrid" and rep["totals"]["rejected"] == 0
    checks = {c["check"]: c["passed"] for c in rep["reconciliation"]}
    assert checks["records == valid + rejected (every domain)"] is True
    assert checks["gold: ML training set has >= model.min_rows rows and all 4 labels"] is not None   # applies again
    dist = rep["gold"]["risk_category_distribution"]
    assert sum(v for k, v in dist.items() if k != "Insufficient data") > 0, dist
    src = {d: {r["data_source"] for r in spark.read.parquet(lake(cfg, "silver", d)).select("data_source").distinct().collect()}
           for d in ("weather", "water_level", "traffic", "incidents")}
    assert src["weather"] == {"era5-open-meteo"}
    assert src["water_level"] == src["traffic"] == src["incidents"] == {HYBRID_SOURCE}
