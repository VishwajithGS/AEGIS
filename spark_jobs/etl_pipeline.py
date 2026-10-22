"""AEGIS batch ETL: bronze -> silver -> gold, with data-quality reporting.

Bronze  raw generated JSONL files (cold storage) and, when present, the raw
        Kafka archive written by the streaming job. Materialised unchanged as
        ``lake/bronze/files``.
Silver  canonical per-domain tables (``lake/silver/<domain>``, partitioned by
        event_date) after validation, unit/time/category standardisation and
        de-duplication on ``event_id``. Rejected records go to
        ``lake/quarantine/batch`` with their reasons.
Gold    zone_hourly, zone_hazard_history, zone_risk_hourly, zone_daily,
        incident_daily, incident_hour_of_day, traffic_hazard_correlation,
        rainfall_disruption_bands, ml_training_flood_severity, dq_report.

    python -m spark_jobs.etl_pipeline                       # files (+ stream bronze if present)
    python -m spark_jobs.etl_pipeline --write-hbase         # also load HBase
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from aegis.config import data_path, get_config
from aegis.logging_config import setup_logging
from spark_jobs import hbase_sink, transforms as TR
from spark_jobs.common import DOMAINS, Timer, build_spark, lake, path_exists, remove_dir, reports_dir, write_json
from spark_jobs.features import point_in_time_features
from spark_jobs.risk_analytics import aggregate_signals, hazard_index, reports_observed, score, zone_signals
from spark_jobs.spatial_analytics import nearest_station_map, zones_df

log = setup_logging("etl")

FILE_TOPIC_KEYS = {"weather": "weather", "water_level": "water_level", "traffic": "traffic", "incidents": "incidents"}
# generator anomaly kind -> reason code detected by the ETL
ANOMALY_TO_REASON = {
    "malformed_json": "malformed_json", "missing_required": "missing_required", "out_of_range": "out_of_range",
    "invalid_type": "invalid_type", "invalid_coordinates": "invalid_coordinates",
    "outside_study_region": "outside_study_region", "unknown_schema_version": "unknown_schema_version",
}


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
def load_file_bronze(spark: SparkSession, cfg: dict, input_dir: Path) -> Optional[DataFrame]:
    frames = []
    for key, topic_key in FILE_TOPIC_KEYS.items():
        p = input_dir / f"{key}.jsonl"
        if not p.exists():
            continue
        frames.append(spark.read.text(str(p)).select(
            F.col("value"), F.lit(cfg["kafka"]["topics"][topic_key]).alias("topic"),
            F.lit(None).cast("int").alias("kafka_partition"), F.lit(None).cast("long").alias("kafka_offset"),
            F.lit(None).cast("timestamp").alias("ingest_time"), F.lit(f"file:{p.name}").alias("origin"),
            F.monotonically_increasing_id().alias("source_seq")))
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out


def load_stream_bronze(spark: SparkSession, cfg: dict) -> Optional[DataFrame]:
    path = lake(cfg, "bronze", "kafka_raw")
    if not path_exists(path):
        return None
    return spark.read.parquet(path).select(
        "value", "topic", "kafka_partition", "kafka_offset", F.col("kafka_timestamp").alias("ingest_time"),
        F.lit("kafka").alias("origin"), F.col("kafka_offset").alias("source_seq"))


def write_table(df: DataFrame, path: str, partition_by: Optional[str] = None, files: int = 1) -> int:
    """Overwrite a Parquet table; an empty result still produces a readable,
    schema-only table (real-data runs can lack whole domains). Returns the row count."""
    n = df.count()
    if n == 0:
        remove_dir(path)
        df.sparkSession.createDataFrame([], df.schema).coalesce(1).write.mode("overwrite").parquet(path)
        return 0
    w = (df.repartition(partition_by) if partition_by else df.coalesce(files)).write.mode("overwrite") \
        .option("partitionOverwriteMode", "static")
    (w.partitionBy(partition_by) if partition_by else w).parquet(path)
    return n


# ---------------------------------------------------------------------------
# Gold builders
# ---------------------------------------------------------------------------
def build_zone_hourly(spark: SparkSession, cfg: dict, canon_valid: DataFrame) -> DataFrame:
    wmap = nearest_station_map(spark, cfg, "weather")
    lmap = nearest_station_map(spark, cfg, "water_level")
    hourly = aggregate_signals(zone_signals(canon_valid, wmap, lmap), "1 hour", reports_observed(cfg))
    return (hourly.withColumnRenamed("window_start", "hour").drop("window_end")
            .withColumn("rain_mm", F.round(F.col("rainfall_avg_mm_hr") * 1.0, 3))
            .withColumn("event_date", F.to_date("hour")))


def build_hazard_history(spark: SparkSession, cfg: dict, zone_hourly: DataFrame) -> DataFrame:
    per_zone = zone_hourly.groupBy("zone_id").agg(
        F.sum("archived_flood_incidents").alias("archived_flood_incidents"),
        F.sum("archived_incidents").alias("archived_incidents"),
        F.sum("reports").alias("public_reports"))
    all_zones = zones_df(spark, cfg).select("zone_id", "center_lat", "center_lon")
    joined = all_zones.join(per_zone, "zone_id", "left").fillna(
        {"archived_flood_incidents": 0, "archived_incidents": 0, "public_reports": 0})
    return hazard_index(joined)


def build_ml_training(canon_valid: DataFrame, zone_hourly: DataFrame) -> DataFrame:
    inc = canon_valid.filter((F.col("domain") == "incidents"))
    targets = inc.filter((F.col("source_type") == "historical_incident") & F.col("is_flood_related")
                         & F.col("severity").isNotNull() & F.col("zone_id").isNotNull()).select(
        F.col("incident_id").alias("target_id"), "zone_id", F.col("event_time").alias("t"),
        F.col("severity").alias("label"), "category")
    reports = inc.filter((F.col("source_type") == "public_report") & F.col("zone_id").isNotNull()
                         & (F.col("verification_status") != "rejected")).select("zone_id", "event_time")
    buckets = zone_hourly.withColumnRenamed("hour", "window_start")
    feats = point_in_time_features(targets, buckets, reports, 3600)
    return (feats.withColumnRenamed("target_id", "incident_id").withColumnRenamed("t", "event_time")
            .withColumn("label_provenance", F.lit("synthetic: producers.data_generator.severity_rule"))
            .withColumn("event_date", F.to_date("event_time")))


def build_trends(silver_incidents: DataFrame) -> tuple[DataFrame, DataFrame]:
    located = silver_incidents.filter(F.col("verification_status") != "rejected")
    daily = (located.groupBy("event_date", "category", "source_type")
             .agg(F.count(F.lit(1)).alias("incidents"),
                  F.sum(F.when(F.col("severity").isin("High", "Critical"), 1).otherwise(0)).alias("high_or_critical"))
             .orderBy("event_date", "category"))
    hod = (located.withColumn("hour_of_day_ist", F.hour(F.from_utc_timestamp("event_time", "Asia/Kolkata")))
           .groupBy("hour_of_day_ist", "category").agg(F.count(F.lit(1)).alias("incidents"))
           .orderBy("hour_of_day_ist", "category"))
    return daily, hod


def build_correlations(zone_hourly: DataFrame) -> tuple[DataFrame, DataFrame]:
    zh = zone_hourly.filter(F.col("traffic_obs") > 0)
    per_zone = (zh.groupBy("zone_id").agg(
        F.round(F.corr("rainfall_avg_mm_hr", "disrupted_ratio"), 4).alias("corr_rain_disruption"),
        F.round(F.corr("level_ratio_max", "speed_ratio_avg"), 4).alias("corr_level_speed"),
        F.round(F.corr("weighted_reports", "disrupted_ratio"), 4).alias("corr_reports_disruption"),
        F.count(F.lit(1)).alias("hours"))
        .unionByName(zh.agg(
            F.round(F.corr("rainfall_avg_mm_hr", "disrupted_ratio"), 4).alias("corr_rain_disruption"),
            F.round(F.corr("level_ratio_max", "speed_ratio_avg"), 4).alias("corr_level_speed"),
            F.round(F.corr("weighted_reports", "disrupted_ratio"), 4).alias("corr_reports_disruption"),
            F.count(F.lit(1)).alias("hours")).withColumn("zone_id", F.lit("ALL"))))
    band = (F.when(F.col("rainfall_avg_mm_hr").isNull(), "no_weather_obs")
            .when(F.col("rainfall_avg_mm_hr") < 0.5, "0_dry").when(F.col("rainfall_avg_mm_hr") < 2.5, "1_light")
            .when(F.col("rainfall_avg_mm_hr") < 10, "2_moderate").when(F.col("rainfall_avg_mm_hr") < 25, "3_heavy")
            .otherwise("4_very_heavy"))
    bands = (zh.withColumn("rain_band", band).groupBy("rain_band")
             .agg(F.round(F.avg("disrupted_ratio"), 4).alias("avg_disrupted_ratio"),
                  F.round(F.avg("speed_ratio_avg"), 4).alias("avg_speed_ratio"),
                  F.round(F.avg("weighted_reports"), 4).alias("avg_weighted_reports"),
                  F.count(F.lit(1)).alias("zone_hours"))
             .orderBy("rain_band"))
    return per_zone, bands


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------
def _rows_to_nested(rows, k1: str, k2: str, v: str = "count") -> dict:
    out: dict = {}
    for r in rows:
        out.setdefault(r[k1], {})[r[k2]] = int(r[v])
    return out


def dq_report(canon: DataFrame, dedup_counts: dict, silver_counts: dict, raw_by_topic: dict,
              unknown_topic: int, manifest: Optional[dict], timer: Timer, file_duplicates: int = 0) -> dict:
    totals = {r["domain"]: {"records": int(r["records"]), "valid": int(r["valid"]), "rejected": int(r["rejected"])}
              for r in TR.dq_counts(canon).collect()}
    reasons = _rows_to_nested(TR.reason_counts(canon).collect(), "domain", "reason")
    details = _rows_to_nested(TR.reason_detail_counts(canon).collect(), "domain", "reason_detail")
    flags = _rows_to_nested(TR.flag_counts(canon).collect(), "domain", "flag")
    domains = {}
    for d in DOMAINS:
        t = totals.get(d, {"records": 0, "valid": 0, "rejected": 0})
        missing = {k.split(":", 1)[1]: v for k, v in details.get(d, {}).items() if k.startswith("missing_required:")}
        domains[d] = {**t, "duplicates_removed": dedup_counts.get(d, 0), "silver_rows": silver_counts.get(d, 0),
                      "rejected_by_reason": reasons.get(d, {}), "rejected_by_detail": details.get(d, {}),
                      "missing_required_by_field": missing, "valid_record_flags": flags.get(d, {})}
    grand = {k: sum(v[k] for v in domains.values()) for k in ("records", "valid", "rejected", "duplicates_removed",
                                                               "silver_rows")}
    checks = [
        {"check": "records == valid + rejected (every domain)",
         "passed": all(v["records"] == v["valid"] + v["rejected"] for v in domains.values())},
        {"check": "valid - duplicates_removed == silver_rows (every domain)",
         "passed": all(v["valid"] - v["duplicates_removed"] == v["silver_rows"] for v in domains.values())},
        {"check": "raw input == parsed records + unknown-topic records",
         "passed": sum(raw_by_topic.values()) == grand["records"] + unknown_topic,
         "detail": {"raw": sum(raw_by_topic.values()), "parsed": grand["records"], "unknown_topic": unknown_topic}},
    ]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "run_type": "batch", "duration_s": timer.seconds(),
        "input": {"raw_records": sum(raw_by_topic.values()), "by_topic": raw_by_topic, "unknown_topic": unknown_topic},
        "totals": grand, "domains": domains, "reconciliation": checks,
    }
    report["data_mode"] = (manifest or {}).get("data_mode", "synthetic")
    if manifest and "injected_anomalies" in manifest:
        file_reasons: dict = {}
        for r in (TR.reason_counts(canon.filter(F.col("origin").startswith("file:")))
                  .groupBy("reason").agg(F.sum("count").alias("n")).collect()):
            file_reasons[r["reason"]] = int(r["n"])
        inj = manifest.get("injected_anomalies", {})
        cmp = {k: {"injected": int(inj.get(k, 0)), "detected": int(file_reasons.get(reason, 0))}
               for k, reason in ANOMALY_TO_REASON.items()}
        cmp["duplicate"] = {"injected": int(inj.get("duplicate", 0)), "detected": int(file_duplicates)}
        report["injected_vs_detected"] = cmp
        report["injected_vs_detected_note"] = (
            "Counts for the generated files only. 'detected' counts records whose reasons include the code; "
            "duplicates are compared with records removed by event_id de-duplication.")
        report["reconciliation"].append({"check": "every injected defect detected with the expected reason",
                                         "passed": all(v["injected"] == v["detected"] for v in cmp.values())})
    elif manifest:
        report["reconciliation"].append({
            "check": "every injected defect detected with the expected reason", "passed": None,
            "detail": "not applicable: the input manifest records no injected defects (real or hybrid data)"})
    return report


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(spark: SparkSession, cfg: dict, input_dir: Path, include_stream: bool = True,
        write_hbase: bool = False, hbase_sensor_days: int = 2) -> dict:
    timer = Timer()
    file_raw = load_file_bronze(spark, cfg, input_dir)
    stream_raw = load_stream_bronze(spark, cfg) if include_stream else None
    if file_raw is None and stream_raw is None:
        raise SystemExit(f"no input: generate data first (no JSONL in {input_dir} and no stream bronze)")
    raw = file_raw if stream_raw is None else (stream_raw if file_raw is None else file_raw.unionByName(stream_raw))
    raw = raw.persist(StorageLevel.MEMORY_AND_DISK)
    raw_by_topic = {r["topic"]: int(r["count"]) for r in raw.groupBy("topic").count().collect()}
    log.info(f"bronze loaded: {raw_by_topic}")
    if file_raw is not None:
        (file_raw.repartition("topic").write.mode("overwrite").option("partitionOverwriteMode", "static")
         .partitionBy("topic").parquet(lake(cfg, "bronze", "files")))

    unknown_topic = TR.unknown_topic_records(raw, cfg).count()
    canon = TR.union_canonical(TR.canonicalize_all(raw, cfg).values()).persist(StorageLevel.MEMORY_AND_DISK)
    valid = canon.filter(F.col("is_valid"))
    dedup = TR.dedup_batch(valid).persist(StorageLevel.MEMORY_AND_DISK)
    valid_counts = {r["domain"]: int(r["count"]) for r in valid.groupBy("domain").count().collect()}
    dedup_n = {r["domain"]: int(r["count"]) for r in dedup.groupBy("domain").count().collect()}
    dup_counts = {d: valid_counts.get(d, 0) - dedup_n.get(d, 0) for d in DOMAINS}

    # ---- silver -----------------------------------------------------------
    silver_counts = {}
    for d in DOMAINS:
        view = TR.domain_view(dedup, d)
        write_table(view, lake(cfg, "silver", d), partition_by="event_date")
        silver_counts[d] = spark.read.parquet(lake(cfg, "silver", d)).count()   # read back = verification
    write_table(TR.quarantine_view(canon, int(cfg["streaming"]["max_raw_value_chars"])),
                lake(cfg, "quarantine", "batch"), partition_by="domain")
    log.info(f"silver written: {silver_counts}; duplicates removed: {dup_counts}")

    # ---- gold -------------------------------------------------------------
    zone_hourly = build_zone_hourly(spark, cfg, dedup).persist(StorageLevel.MEMORY_AND_DISK)
    zone_hourly.coalesce(4).write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(lake(cfg, "gold", "zone_hourly"))
    hazard = build_hazard_history(spark, cfg, zone_hourly)
    hazard.coalesce(1).write.mode("overwrite").parquet(lake(cfg, "gold", "zone_hazard_history"))
    hz = spark.read.parquet(lake(cfg, "gold", "zone_hazard_history")).select("zone_id", "historical_hazard_index")
    risk_hourly = (score(zone_hourly.join(F.broadcast(hz), "zone_id", "left"), cfg)
                   .withColumn("assessed_at", F.current_timestamp())
                   .withColumn("data_origin", F.lit("batch-historical")))
    risk_hourly.coalesce(4).write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(lake(cfg, "gold", "zone_risk_hourly"))
    risk_hourly = spark.read.parquet(lake(cfg, "gold", "zone_risk_hourly"))

    zone_daily = (zone_hourly.groupBy("zone_id", "event_date").agg(
        F.round(F.sum("rain_mm"), 2).alias("rain_mm"), F.round(F.max("rainfall_max_mm_hr"), 2).alias("rain_peak_mm_hr"),
        F.round(F.max("level_ratio_max"), 3).alias("level_ratio_peak"),
        F.round(F.avg("disrupted_ratio"), 4).alias("disrupted_ratio_avg"),
        F.sum("reports").alias("reports"), F.sum("archived_incidents").alias("archived_incidents"))
        .join(risk_hourly.groupBy("zone_id", F.to_date("hour").alias("event_date")).agg(
            F.round(F.max("risk_score"), 2).alias("risk_peak"), F.round(F.avg("risk_score"), 2).alias("risk_avg")),
            ["zone_id", "event_date"], "left"))
    zone_daily.coalesce(2).write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(lake(cfg, "gold", "zone_daily"))

    silver_inc = spark.read.parquet(lake(cfg, "silver", "incidents"))
    daily, hod = build_trends(silver_inc)
    write_table(daily, lake(cfg, "gold", "incident_daily"))
    write_table(hod, lake(cfg, "gold", "incident_hour_of_day"))
    corr, bands = build_correlations(zone_hourly)
    write_table(corr, lake(cfg, "gold", "traffic_hazard_correlation"))
    write_table(bands, lake(cfg, "gold", "rainfall_disruption_bands"))

    ml = build_ml_training(dedup, zone_hourly)
    write_table(ml, lake(cfg, "gold", "ml_training_flood_severity"))
    ml = spark.read.parquet(lake(cfg, "gold", "ml_training_flood_severity"))
    ml_rows = ml.count()
    label_dist = {r["label"]: int(r["count"]) for r in ml.groupBy("label").count().collect()}

    # ---- data quality -----------------------------------------------------
    manifest_path = input_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
    file_valid = valid.filter(F.col("origin").startswith("file:"))
    file_dups = file_valid.count() - file_valid.select("event_id").distinct().count()
    report = dq_report(canon, dup_counts, silver_counts, raw_by_topic, unknown_topic, manifest, timer, file_dups)
    silver_all = TR.union_canonical([spark.read.parquet(lake(cfg, "silver", d)).withColumn("domain", F.lit(d))
                                     for d in DOMAINS])
    v = silver_all.agg(
        F.sum(F.col("event_time").isNull().cast("int")).alias("null_time"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("null_id"),
        (F.count(F.lit(1)) - F.countDistinct("event_id")).alias("dup_ids"),
        F.sum((F.col("zone_id").isNull() & (F.col("domain") != "incidents")).cast("int")).alias("sensor_no_zone"),
    ).first()
    rs = risk_hourly.agg(F.min("risk_score"), F.max("risk_score")).first()
    report["reconciliation"] += [
        {"check": "silver: no NULL event_time / event_id", "passed": v["null_time"] == 0 and v["null_id"] == 0},
        {"check": "silver: event_id unique across all domains", "passed": v["dup_ids"] == 0,
         "detail": int(v["dup_ids"])},
        {"check": "silver: every sensor observation has a zone", "passed": v["sensor_no_zone"] == 0},
        {"check": "gold: risk_score within [0, 100]",
         "passed": (rs[0] is None or rs[0] >= 0) and (rs[1] is None or rs[1] <= 100),
         "detail": {"min": rs[0], "max": rs[1]}},
        ({"check": "gold: ML training set has >= model.min_rows rows and all 4 labels",
          "passed": ml_rows >= int(cfg["model"]["min_rows"]) and len(label_dist) == 4,
          "detail": {"rows": ml_rows, "labels": label_dist}} if report["data_mode"] in ("synthetic", "hybrid") else
         {"check": "gold: ML training set has >= model.min_rows rows and all 4 labels", "passed": None,
          "detail": f"not applicable in {report['data_mode']} mode: no verified incident severity labels "
                    f"(see docs/dataset_migration_plan.md, ML audit); rows={ml_rows}"}),
    ]
    report["gold"] = {
        "zone_hourly_rows": zone_hourly.count(), "zone_risk_hourly_rows": risk_hourly.count(),
        "risk_category_distribution": {r["risk_category"]: int(r["count"])
                                       for r in risk_hourly.groupBy("risk_category").count().collect()},
        "ml_training_rows": ml_rows, "ml_label_distribution": label_dist,
    }
    spark.createDataFrame([(json.dumps(report), report["generated_at"])], "report_json string, generated_at string") \
        .coalesce(1).write.mode("append").parquet(lake(cfg, "gold", "dq_report"))

    # ---- optional HBase load ------------------------------------------------
    if write_hbase:
        report["hbase"] = load_hbase(spark, cfg, dedup, risk_hourly, hbase_sensor_days)

    report["duration_s"] = timer.seconds()
    write_json(reports_dir(cfg) / "dq_report_batch.json", report)
    for df in (raw, canon, dedup, zone_hourly):
        df.unpersist()
    return report


def load_hbase(spark: SparkSession, cfg: dict, dedup: DataFrame, risk_hourly: DataFrame, sensor_days: int) -> dict:
    h = hbase_sink.hbase_settings(cfg)
    if h is None:
        return {"status": "disabled in config"}
    out: dict = {}
    try:
        from aegis.storage.hbase_repository import HBaseRepository
        HBaseRepository.from_config(cfg).ensure_tables()
        inc = hbase_sink.prepare_incident(dedup.filter(F.col("domain") == "incidents"))
        out["incident_records_written"] = hbase_sink.write(inc, "incident_records", hbase_sink.INCIDENT_SPEC,
                                                           hbase_sink.incident_key, h)
        out["incident_records_verify"] = hbase_sink.verify_sample(inc, "incident_records", hbase_sink.incident_key, h,
                                                                  "incident", "category")
        max_t = dedup.agg(F.max("event_time")).first()[0]
        sensors = hbase_sink.prepare_sensor(dedup.filter(
            (F.col("domain") != "incidents") & (F.col("event_time") >= F.lit(max_t) - F.expr(f"INTERVAL {int(sensor_days)} DAYS"))))
        out["sensor_observations_written"] = hbase_sink.write(sensors, "sensor_observations", hbase_sink.SENSOR_SPEC,
                                                              hbase_sink.sensor_key, h)
        out["sensor_observations_verify"] = hbase_sink.verify_sample(sensors, "sensor_observations",
                                                                     hbase_sink.sensor_key, h, "metadata", "event_id")
        zr = hbase_sink.prepare_zone_risk(risk_hourly.withColumnRenamed("hour", "window_start")
                                          .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 1 HOUR"))
                                          .withColumn("risk_series", F.lit("hourly")))
        out["zone_risk_history_written"] = hbase_sink.write(zr, "zone_risk_history", hbase_sink.ZONE_RISK_SPEC,
                                                            hbase_sink.zone_risk_key, h)
        out["zone_risk_history_verify"] = hbase_sink.verify_sample(zr, "zone_risk_history", hbase_sink.zone_risk_key,
                                                                   h, "risk", "risk_category")
        out["status"] = "ok"
    except Exception as exc:  # noqa: BLE001 - surface any HBase failure in the report
        log.error(f"HBase load failed: {exc}")
        out["status"] = f"failed: {exc}"
    return out


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="AEGIS batch ETL (bronze -> silver -> gold)")
    ap.add_argument("--input", default=str(data_path(cfg, "generated_dir", "historical")))
    ap.add_argument("--no-stream-bronze", action="store_true", help="ignore the streaming Kafka archive")
    ap.add_argument("--write-hbase", action="store_true")
    ap.add_argument("--hbase-sensor-days", type=int, default=2)
    args = ap.parse_args()
    spark = build_spark("etl", cfg)
    report = run(spark, cfg, Path(args.input), not args.no_stream_bronze, args.write_hbase, args.hbase_sensor_days)
    print(json.dumps({"duration_s": report["duration_s"], "totals": report["totals"],
                      "reconciliation": report["reconciliation"],
                      "injected_vs_detected": report.get("injected_vs_detected"),
                      "gold": report["gold"], "hbase": report.get("hbase")}, indent=2, default=str))
    spark.stop()
    failed = [c["check"] for c in report["reconciliation"] if c["passed"] is False]
    hbase_failed = args.write_hbase and (report.get("hbase") or {}).get("status") != "ok"
    if failed or hbase_failed:
        log.error(f"ETL finished with problems: failed checks={failed} hbase_load_failed={hbase_failed}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
