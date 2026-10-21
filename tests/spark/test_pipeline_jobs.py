"""Batch ETL, RDD demo, Spark SQL and risk scoring on a small generated dataset."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from producers import formats
from producers.data_generator import generate_historical

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def etl_result(spark, tmp_path_factory):
    from aegis.config import load_config
    root = tmp_path_factory.mktemp("lake")
    cfg = load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(root / "data"), "AEGIS__MODEL__MIN_ROWS": "1"})
    gen_dir = root / "data" / "generated" / "historical"
    manifest = generate_historical(cfg, gen_dir, seed=11, days=3, step_minutes=30,
                                   end=datetime(2026, 10, 15, tzinfo=formats.IST), anomaly_rate=0.05,
                                   reference_dir=root / "data" / "reference")
    from spark_jobs.etl_pipeline import run
    report = run(spark, cfg, gen_dir, include_stream=False, write_hbase=False)
    return cfg, report, manifest, gen_dir


def test_etl_reconciliation_and_injected_defects(etl_result):
    _, report, manifest, _ = etl_result
    failed = [c for c in report["reconciliation"] if not c["passed"] and "ML training" not in c["check"]]
    assert not failed, failed
    for kind, v in report["injected_vs_detected"].items():
        assert v["injected"] == v["detected"], (kind, v)
    assert report["totals"]["rejected"] > 0 and report["totals"]["duplicates_removed"] > 0


def test_etl_outputs_readable(spark, etl_result):
    from spark_jobs.common import lake
    cfg = etl_result[0]
    for parts in (("silver", "weather"), ("silver", "incidents"), ("gold", "zone_hourly"), ("gold", "zone_risk_hourly"),
                  ("gold", "ml_training_flood_severity"), ("quarantine", "batch"), ("bronze", "files")):
        df = spark.read.parquet(lake(cfg, *parts))
        assert df.count() > 0, parts
    risk = spark.read.parquet(lake(cfg, "gold", "zone_risk_hourly"))
    assert risk.filter("risk_score < 0 OR risk_score > 100").count() == 0
    hourly = spark.read.parquet(lake(cfg, "gold", "zone_hourly"))
    assert hourly.select("zone_id").distinct().count() <= 40


def test_ml_features_use_only_past_data(spark, etl_result):
    from pyspark.sql import functions as F
    from spark_jobs.common import lake
    cfg = etl_result[0]
    ml = spark.read.parquet(lake(cfg, "gold", "ml_training_flood_severity"))
    assert "label" in ml.columns and "severity" not in ml.columns
    assert ml.filter(F.col("reports_6h") < 0).count() == 0
    assert set(r["label"] for r in ml.select("label").distinct().collect()) <= {"Low", "Moderate", "High", "Critical"}


def test_rdd_demo_runs_and_matches_dataframe(spark, etl_result):
    from spark_jobs.rdd_demo import run
    cfg, _, _, gen_dir = etl_result
    res = run(spark, cfg, gen_dir)
    assert res["verification_rdd_equals_dataframe"] is True
    assert res["valid_records"] + res["malformed_or_incomplete"] == res["raw_lines"]
    assert {o["type"] for o in res["operations"]} == {"transformation", "action"}
    names = {o["operation"] for o in res["operations"]}
    assert {"map", "filter", "flatMap", "reduceByKey", "distinct", "sortBy", "count", "collect", "take", "reduce",
            "first", "aggregateByKey", "mapValues"} <= names


def test_sql_queries_execute(spark, etl_result):
    from spark_jobs.common import lake
    from spark_jobs.spatial_analytics import zones_df
    from spark_jobs.sql_analytics import QUERIES, VIEWS
    cfg = etl_result[0]
    for view, parts in VIEWS.items():
        spark.read.parquet(lake(cfg, *parts)).createOrReplaceTempView(view)
    zones_df(spark, cfg).createOrReplaceTempView("zones")
    for name, _, sql in QUERIES:
        rows = spark.sql(sql).collect()
        assert rows is not None, name
        if name in ("q1_incidents_by_category", "q3_rainfall_6h_windows", "q6_zone_risk_ranking", "q8_data_quality"):
            assert len(rows) > 0, name


def test_risk_udf_matches_python_reference(spark, cfg):
    from aegis.risk import assess
    from spark_jobs.risk_analytics import score
    rows = [("A", 30.0, 0.9, 2.0, 0.5, 0.3), ("B", None, None, 0.0, None, None), ("C", 60.0, None, 6.0, 0.9, 1.0)]
    df = spark.createDataFrame(rows, "zone_id string, rainfall_avg_mm_hr double, level_ratio_max double, "
                                     "weighted_reports double, disrupted_ratio double, historical_hazard_index double")
    got = {r["zone_id"]: r for r in score(df, cfg).collect()}
    for z, rain, lvl, rep, dis, hist in rows:
        ref = assess({"rainfall_avg_mm_hr": rain, "level_ratio_max": lvl, "weighted_reports": rep,
                      "disrupted_ratio": dis, "historical_hazard_index": hist}, cfg["risk"])
        assert got[z]["risk_score"] == ref.risk_score and got[z]["risk_category"] == ref.risk_category
        assert got[z]["coverage"] == ref.coverage


def test_zone_signals_with_missing_domains(spark, cfg):
    """Regression: silver read back with only some domains present (e.g. no live
    incident reports yet) must still integrate; absent indicators are NULL/zero."""
    import json as _json
    from pyspark.sql import functions as F
    from spark_jobs import transforms as TR
    from spark_jobs.risk_analytics import aggregate_signals, zone_signals
    ev = {"event_id": "WX-only-1", "source_type": "weather", "source_id": "WXM-01", "schema_version": "wx-metric-1",
          "synthetic": True, "event_time": "2026-10-20T15:30:00+05:30", "lat": 13.05, "lon": 80.22,
          "temperature_c": 27.0, "humidity_pct": 90, "rainfall_mm_hr": 12.0, "condition": "Rain"}
    raw = spark.createDataFrame([(_json.dumps(ev), "weather-events", None, None, None, "test", 0)],
                                "value string, topic string, kafka_partition int, kafka_offset long, "
                                "ingest_time timestamp, origin string, source_seq long")
    silver_weather = TR.domain_view(TR.canonicalize(raw, "weather", cfg), "weather").withColumn("domain", F.lit("weather"))
    canon = TR.with_all_domain_columns(TR.union_canonical([silver_weather]))
    rows = aggregate_signals(zone_signals(canon, None, None), "1 hour").collect()
    assert len(rows) == 1
    r = rows[0]
    assert r["rainfall_avg_mm_hr"] == 12.0 and r["weather_obs"] == 1
    assert r["level_ratio_max"] is None and r["water_obs"] == 0 and r["reports"] == 0


def test_kafka_records_from_offset_ranges():
    from spark_jobs.streaming_pipeline import kafka_records_in_batch
    src = [{"startOffset": {"weather-events": {"0": 10, "1": 5}, "traffic-events": {"0": 0}},
            "endOffset": {"weather-events": {"0": 14, "1": 5}, "traffic-events": {"0": 7}}}]
    assert kafka_records_in_batch(src) == 11
    as_strings = [{"startOffset": '{"t": {"0": 1}}', "endOffset": '{"t": {"0": 4}}'}]
    assert kafka_records_in_batch(as_strings) == 3
    assert kafka_records_in_batch([{"startOffset": None, "endOffset": {"t": {"0": 4}}}]) is None
    assert kafka_records_in_batch([]) is None


def test_restart_backoff_schedule():
    from spark_jobs.streaming_pipeline import restart_delay_s
    assert [restart_delay_s(i) for i in range(1, 7)] == [10, 20, 40, 60, 60, 60]
