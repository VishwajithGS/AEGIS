"""Inference with the persisted Spark MLlib severity model.

``SeverityModel`` loads the latest (or a given) PipelineModel and predicts
from validated feature records; it is used by this CLI and by the FastAPI
backend. The CLI can also score every zone from live (streamed) data:

    python -m spark_jobs.model_inference --json '{"rain_3h_mm": 42, "rain_max_1h_mm_hr": 30, ...}'
    python -m spark_jobs.model_inference --zones            # live zone conditions -> predictions
    python -m spark_jobs.model_inference --zones --source batch

Predictions are ML estimates trained on SYNTHETIC labels. They are not
observed severities and not official warnings, and they are independent of
the rule-based zone risk score.
"""
from __future__ import annotations

import argparse
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aegis.config import data_path, get_config
from aegis.ml_contract import FeatureValidationError, validate_features  # noqa: F401 (re-exported)
from aegis.logging_config import setup_logging
from spark_jobs.common import DOMAINS, build_spark, lake, path_exists, write_json

log = setup_logging("ml-inference")

class ModelUnavailable(RuntimeError):
    pass


INSUFFICIENT_WEATHER = "insufficient_data: no weather observation in the feature window"


EXAMPLE_RECORD = {"rain_3h_mm": 55.0, "rain_max_1h_mm_hr": 32.0, "level_ratio_max_3h": 0.95, "humidity_avg_3h": 95.0,
                  "disrupted_ratio_1h": 0.5, "speed_ratio_avg_1h": 0.4, "reports_6h": 4, "water_obs_available": 1,
                  "traffic_obs_available": 1}


class SeverityModel:
    def __init__(self, spark: SparkSession, model_dir: Path):
        meta_path = model_dir / "metadata.json"
        if not (model_dir / "pipeline").exists() or not meta_path.exists():
            raise ModelUnavailable(f"no trained model at {model_dir}")
        self.spark = spark
        self.dir = model_dir
        self.meta = json.loads(meta_path.read_text(encoding="utf-8"))
        self.features: list[str] = self.meta["features"]
        self.labels: list[str] = self.meta["labels"]
        self.version: str = self.meta["version"]
        self.model = PipelineModel.load(str(model_dir / "pipeline"))
        self._lock = threading.Lock()
        self.schema = T.StructType([T.StructField("row_id", T.StringType())]
                                   + [T.StructField(f, T.DoubleType(), True) for f in self.features])

    @staticmethod
    def latest_dir(cfg: dict) -> Optional[Path]:
        root = data_path(cfg, "models_dir", cfg["model"]["name"])
        pointer = root / "latest.json"
        if not pointer.exists():
            return None
        version = json.loads(pointer.read_text(encoding="utf-8"))["version"]
        return root / version

    @classmethod
    def load_latest(cls, spark: SparkSession, cfg: dict) -> "SeverityModel":
        d = cls.latest_dir(cfg)
        if d is None:
            raise ModelUnavailable("no model has been trained yet (run scripts/train_model.ps1)")
        return cls(spark, d)

    def predict_frame(self, df: DataFrame) -> DataFrame:
        others = [c for c in df.columns if c not in self.features]
        out = self.model.transform(df.select(*others, *[F.col(f).cast("double").alias(f) for f in self.features]))
        to_arr = F.udf(lambda v: [round(float(x), 4) for x in v.toArray()] if v is not None else None,
                       T.ArrayType(T.DoubleType()))
        return out.withColumn("probabilities", to_arr("probability"))

    def predict_records(self, records: list[dict]) -> list[dict]:
        clean = [validate_features(r, self.features) for r in records]
        rows = [tuple([str(i)] + [c[f] for f in self.features]) for i, c in enumerate(clean)]
        with self._lock:
            df = self.spark.createDataFrame(rows, self.schema)
            res = {r["row_id"]: r for r in self.predict_frame(df).select("row_id", "predicted_label", "probabilities").collect()}
        now = datetime.now(timezone.utc).isoformat()
        out = []
        for i, c in enumerate(clean):
            r = res[str(i)]
            out.append({
                "predicted_severity": r["predicted_label"],
                "probabilities": dict(zip(self.labels, r["probabilities"])),
                "features": c,
                "imputed_features": [f for f in self.features if c[f] is None],
                "model_version": self.version, "label_provenance": self.meta["label_provenance"],
                "predicted_at": now,
                "note": "ML estimate from a model trained on synthetic labels; not an observed severity or a warning.",
            })
        return out


# ---------------------------------------------------------------------------
# Live zone scoring
# ---------------------------------------------------------------------------
def zone_feature_frame(spark: SparkSession, cfg: dict, source: str, as_of: Optional[datetime],
                       as_of_latest: bool = False) -> tuple[DataFrame, int, datetime]:
    """Point-in-time features for every zone at ``as_of`` from streamed or batch silver data.

    ``as_of`` defaults to now for live data and to the end of the newest bucket for batch
    data. ``as_of_latest`` uses the end of the bucket holding the newest event for live data
    too (useful when the stream is paused, e.g. in the end-to-end check)."""
    from spark_jobs.features import point_in_time_features
    from spark_jobs.risk_analytics import aggregate_signals, zone_signals
    from spark_jobs.spatial_analytics import nearest_station_map, zones_df
    from spark_jobs.transforms import union_canonical, with_all_domain_columns

    base = "silver_stream" if source == "live" else "silver"
    frames = []
    for d in DOMAINS:
        p = lake(cfg, base, d)
        if path_exists(p):
            frames.append(spark.read.parquet(p).withColumn("domain", F.lit(d)))
    if not frames:
        raise ModelUnavailable(f"no {base} data found; start the streaming pipeline / run the ETL")
    canon = with_all_domain_columns(union_canonical(frames))   # domains with no data yet -> NULL columns
    bucket = int(cfg["model"]["live_bucket_seconds"]) if source == "live" else 3600
    if as_of is None:
        if source == "live" and not as_of_latest:
            as_of = datetime.now(timezone.utc)
        else:
            # epoch micros avoids collect() converting the timestamp to the driver's local zone
            latest_us = canon.agg(F.expr("max(unix_micros(event_time))")).first()[0]
            if latest_us is None:
                raise ModelUnavailable(f"no events in {base}")
            as_of = datetime.fromtimestamp((int(latest_us // 1_000_000) // bucket + 1) * bucket, timezone.utc)
    signals = zone_signals(canon, nearest_station_map(spark, cfg, "weather"), nearest_station_map(spark, cfg, "water_level"))
    buckets = aggregate_signals(signals, f"{bucket} seconds")
    reports = canon.filter((F.col("domain") == "incidents") & (F.col("source_type") == "public_report")
                           & F.col("zone_id").isNotNull() & (F.col("verification_status") != "rejected")) \
        .select("zone_id", "event_time")
    targets = zones_df(spark, cfg).select(F.col("zone_id").alias("target_id"), "zone_id") \
        .withColumn("t", F.lit(as_of).cast("timestamp"))
    return point_in_time_features(targets, buckets, reports, bucket), bucket, as_of


def score_zones(spark: SparkSession, cfg: dict, source: str = "live", as_of: Optional[datetime] = None,
                write_hbase: bool = True, as_of_latest: bool = False) -> dict:
    model = SeverityModel.load_latest(spark, cfg)
    feats, bucket, as_of = zone_feature_frame(spark, cfg, source, as_of, as_of_latest)
    now = datetime.now(timezone.utc)
    # Rainfall drives the model, and training incidents almost always have rain data,
    # so imputing it (training median = "rainy") would invent a severity. A zone with
    # no weather observation in its feature window gets no prediction instead.
    no_weather = F.col("rain_3h_mm").isNull() | F.col("rain_max_1h_mm_hr").isNull()
    pred = (model.predict_frame(feats)
            .withColumn("prediction_status", F.when(no_weather, F.lit(INSUFFICIENT_WEATHER)).otherwise(F.lit("ok")))
            .withColumn("predicted_label", F.when(no_weather, F.lit(None).cast("string")).otherwise(F.col("predicted_label")))
            .withColumn("probabilities", F.when(no_weather, F.lit(None).cast("array<double>")).otherwise(F.col("probabilities")))
            .withColumn("prediction_id", F.expr("uuid()"))
            .withColumn("model_version", F.lit(model.version))
            .withColumn("label_provenance", F.lit(model.meta["label_provenance"]))
            .withColumn("input_source", F.lit(f"{source} zone conditions, bucket={bucket}s"))
            .withColumn("predicted_at", F.lit(now).cast("timestamp"))
            .withColumn("features_as_of", F.lit(as_of).cast("timestamp"))
            .select("prediction_id", "zone_id", *model.features, F.col("predicted_label").alias("predicted_severity"),
                    "probabilities", "prediction_status", "model_version", "label_provenance", "input_source", "predicted_at",
                    "features_as_of")).cache()
    pred.withColumn("predicted_date", F.to_date("predicted_at")).write.mode("append") \
        .partitionBy("predicted_date").parquet(lake(cfg, "live", "ml_predictions"))
    from spark_jobs import hbase_sink
    # ISO strings rendered in Spark (UTC); collect() would give driver-local naive datetimes.
    rows = [r.asDict() for r in pred.select(
        *[c for c in pred.columns if c not in ("predicted_at", "features_as_of")],
        hbase_sink.iso("predicted_at").alias("predicted_at"),
        hbase_sink.iso("features_as_of").alias("features_as_of")).collect()]      # one row per zone (40)
    hb = {"status": "skipped"}
    if write_hbase and cfg["hbase"].get("enabled", True):
        try:
            hdf = (pred.withColumn("predicted_at_ms", F.expr("unix_millis(predicted_at)"))
                   .withColumn("predicted_at_iso", hbase_sink.iso("predicted_at"))
                   .withColumn("features_as_of_iso", hbase_sink.iso("features_as_of"))
                   .withColumn("probabilities", F.to_json("probabilities")))
            n = hbase_sink.write(hdf, "model_predictions", hbase_sink.PREDICTION_SPEC, hbase_sink.prediction_key,
                                 dict(cfg["hbase"]))
            hb = {"status": "ok", "rows_written": n}
        except Exception as exc:  # noqa: BLE001 - report, do not hide
            hb = {"status": f"failed: {exc}"}
            log.error(f"HBase prediction write failed: {exc}")
    summary = {"model_version": model.version, "source": source, "bucket_seconds": bucket,
               "features_as_of": rows[0]["features_as_of"] if rows else None,
               "predicted_at": now.isoformat(), "zones": len(rows),
               "distribution": {}, "hbase": hb, "predictions": rows}
    for r in rows:
        key = r["predicted_severity"] or "Insufficient data"
        summary["distribution"][key] = summary["distribution"].get(key, 0) + 1
    write_json(data_path(cfg, "state_dir", "ml_zone_predictions_latest.json"), summary)
    pred.unpersist()
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="AEGIS severity model inference (Spark MLlib)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--json", help="one feature record as JSON")
    g.add_argument("--example", action="store_true", help="predict a built-in heavy-rain example record")
    g.add_argument("--zones", action="store_true", help="score every zone from current conditions")
    ap.add_argument("--source", choices=["live", "batch"], default="live")
    ap.add_argument("--as-of-latest-event", action="store_true",
                    help="live: compute features as of the end of the newest data bucket instead of now")
    ap.add_argument("--no-hbase", action="store_true")
    args = ap.parse_args()
    cfg = get_config()
    spark = build_spark("ml-inference", cfg, shuffle_partitions=4)
    try:
        if args.json or args.example:
            record = json.loads(args.json) if args.json else EXAMPLE_RECORD
            model = SeverityModel.load_latest(spark, cfg)
            print(json.dumps(model.predict_records([record])[0], indent=2))
        else:
            s = score_zones(spark, cfg, args.source, write_hbase=not args.no_hbase,
                            as_of_latest=args.as_of_latest_event)
            print(json.dumps({k: v for k, v in s.items() if k != "predictions"}, indent=2, default=str))
            for p in sorted(s["predictions"], key=lambda r: r["zone_id"])[:10]:
                print(f"  {p['zone_id']}  predicted={str(p['predicted_severity'] or 'insufficient'):12} rain_3h={p['rain_3h_mm']} "
                      f"level={p['level_ratio_max_3h']} reports_6h={p['reports_6h']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
