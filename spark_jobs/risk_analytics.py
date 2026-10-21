"""Zone signal integration and rule-based risk scoring (Spark).

``zone_signals`` maps every validated record to the zone(s) it informs:

* weather   -> zones whose nearest weather station (within radius) is the source
* water     -> zones whose nearest river gauge (within radius) is the source;
               readings flagged ``sensor_fault`` are excluded
* traffic   -> the segment's own zone
* incidents -> the report's own zone (records without coordinates are excluded)

``aggregate_signals`` turns signals into per-zone indicators for a time
bucket, and ``score`` applies ``aegis.risk.assess`` (one formula shared with
the API and tests) through a Python UDF.

    python -m spark_jobs.risk_analytics        # rank zones from the batch gold tables
"""
from __future__ import annotations

import json
from typing import Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aegis.config import get_config
from aegis.logging_config import setup_logging
from aegis.risk import assess

log = setup_logging("risk")

RISK_SCHEMA = T.StructType([
    T.StructField("risk_score", T.DoubleType()),
    T.StructField("risk_category", T.StringType()),
    T.StructField("confidence", T.StringType()),
    T.StructField("coverage", T.DoubleType()),
    T.StructField("contributions", T.StringType()),
    T.StructField("explanation", T.StringType()),
    T.StructField("available_features", T.ArrayType(T.StringType())),
    T.StructField("missing_features", T.ArrayType(T.StringType())),
])
RISK_COLUMNS = [f.name for f in RISK_SCHEMA.fields]


def zone_signals(canon_valid: DataFrame, weather_map: Optional[DataFrame],
                 water_map: Optional[DataFrame]) -> DataFrame:
    """Union of per-zone signal rows from all sources (batch or streaming)."""
    d = F.col("domain")
    w = canon_valid.filter(d == "weather").select("event_time", "source_id", "zone_id",
                                                  "rainfall_mm_hr", "humidity_pct")
    if weather_map is not None:
        w = (w.drop("zone_id").join(F.broadcast(weather_map.select(
            "zone_id", F.col("station_id").alias("source_id"))), "source_id"))
    w = w.select("zone_id", "event_time", F.lit("weather").alias("signal"), "rainfall_mm_hr", "humidity_pct")

    lv = canon_valid.filter((d == "water_level") & ~F.array_contains("dq_flags", "sensor_fault")
                            & F.col("level_ratio").isNotNull()).select("event_time", "source_id", "zone_id", "level_ratio")
    if water_map is not None:
        lv = (lv.drop("zone_id").join(F.broadcast(water_map.select(
            "zone_id", F.col("station_id").alias("source_id"))), "source_id"))
    lv = lv.select("zone_id", "event_time", F.lit("water_level").alias("signal"), "level_ratio")

    t = canon_valid.filter((d == "traffic") & F.col("zone_id").isNotNull()).select(
        "zone_id", "event_time", F.lit("traffic").alias("signal"),
        F.col("is_disrupted").cast("double").alias("disrupted"), "speed_ratio")

    inc = canon_valid.filter((d == "incidents") & F.col("zone_id").isNotNull())
    is_report = F.col("source_type") == "public_report"
    is_archive = F.col("source_type") == "historical_incident"
    i = inc.select(
        "zone_id", "event_time", F.lit("incident").alias("signal"),
        F.when(is_report, F.col("report_weight")).alias("report_weight"),
        (is_report & (F.col("verification_status") != "rejected")).cast("int").alias("is_report"),
        (is_report & F.col("is_flood_related") & (F.col("verification_status") != "rejected")).cast("int").alias("is_flood_report"),
        is_archive.cast("int").alias("is_archived_incident"),
        (is_archive & F.col("is_flood_related")).cast("int").alias("is_archived_flood"),
    )
    return (w.unionByName(lv, allowMissingColumns=True)
            .unionByName(t, allowMissingColumns=True)
            .unionByName(i, allowMissingColumns=True))


def signal_aggregations() -> list[Column]:
    return [
        F.round(F.avg("rainfall_mm_hr"), 3).alias("rainfall_avg_mm_hr"),
        F.round(F.max("rainfall_mm_hr"), 3).alias("rainfall_max_mm_hr"),
        F.count("rainfall_mm_hr").alias("weather_obs"),
        F.round(F.avg("humidity_pct"), 2).alias("humidity_avg"),
        F.round(F.max("level_ratio"), 4).alias("level_ratio_max"),
        F.count("level_ratio").alias("water_obs"),
        F.round(F.avg("disrupted"), 4).alias("disrupted_ratio"),
        F.round(F.avg("speed_ratio"), 4).alias("speed_ratio_avg"),
        F.count("disrupted").alias("traffic_obs"),
        # Reports are counted, so "no report" is an observed zero (see docs: risk model).
        F.round(F.coalesce(F.sum("report_weight"), F.lit(0.0)), 2).alias("weighted_reports"),
        F.coalesce(F.sum("is_report"), F.lit(0)).cast("long").alias("reports"),
        F.coalesce(F.sum("is_flood_report"), F.lit(0)).cast("long").alias("flood_reports"),
        F.coalesce(F.sum("is_archived_incident"), F.lit(0)).cast("long").alias("archived_incidents"),
        F.coalesce(F.sum("is_archived_flood"), F.lit(0)).cast("long").alias("archived_flood_incidents"),
        F.count(F.lit(1)).alias("signal_count"),
    ]


def aggregate_signals(signals: DataFrame, bucket: str) -> DataFrame:
    """Tumbling event-time buckets per zone; ``bucket`` like '1 hour' or '5 minutes'."""
    return (signals.groupBy(F.window("event_time", bucket).alias("w"), "zone_id")
            .agg(*signal_aggregations())
            .select("zone_id", F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "rainfall_avg_mm_hr", "rainfall_max_mm_hr", "weather_obs", "humidity_avg",
                    "level_ratio_max", "water_obs", "disrupted_ratio", "speed_ratio_avg", "traffic_obs",
                    "weighted_reports", "reports", "flood_reports", "archived_incidents",
                    "archived_flood_incidents", "signal_count"))


def make_risk_udf(cfg: dict):
    risk_cfg = cfg["risk"]
    inputs = [f["input"] for f in risk_cfg["features"].values()]

    def _score(*values):
        a = assess(dict(zip(inputs, values)), risk_cfg)
        return (a.risk_score, a.risk_category, a.confidence, a.coverage, json.dumps(a.contributions),
                a.explanation, a.available_features, a.missing_features)
    return F.udf(_score, RISK_SCHEMA), inputs


def score(df: DataFrame, cfg: dict) -> DataFrame:
    """Append risk columns. ``df`` must contain every configured risk input
    column (missing ones are added as NULL = not observed)."""
    udf, inputs = make_risk_udf(cfg)
    for c in inputs:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("double"))
    scored = df.withColumn("_risk", udf(*[F.col(c).cast("double") for c in inputs]))
    return scored.select("*", *[F.col(f"_risk.{c}").alias(c) for c in RISK_COLUMNS]).drop("_risk")


def hazard_index(archived_flood_by_zone: DataFrame) -> DataFrame:
    """historical_hazard_index = zone's archived flood-related incidents / max over zones."""
    mx = archived_flood_by_zone.agg(F.max("archived_flood_incidents")).first()[0] or 0
    return archived_flood_by_zone.withColumn(
        "historical_hazard_index",
        F.round(F.col("archived_flood_incidents") / F.lit(float(mx)), 4) if mx else F.lit(0.0))


def main() -> None:
    from spark_jobs.common import build_spark, lake, read_parquet_if_exists
    cfg = get_config()
    spark = build_spark("risk", cfg)
    risk = read_parquet_if_exists(spark, lake(cfg, "gold", "zone_risk_hourly"))
    if risk is None:
        print("gold/zone_risk_hourly not found - run the batch ETL first")
        spark.stop()
        return
    latest = risk.agg(F.max("hour")).first()[0]
    print(f"\n=== Rule-based zone risk at the latest historical hour ({latest} UTC) ===")
    (risk.filter(F.col("hour") == F.lit(latest))
     .select("zone_id", "risk_score", "risk_category", "confidence", "coverage", "explanation")
     .orderBy(F.desc_nulls_last("risk_score")).show(15, truncate=110))
    print("=== Category distribution over the whole history ===")
    risk.groupBy("risk_category").count().orderBy(F.desc("count")).show()
    spark.stop()


if __name__ == "__main__":
    main()
