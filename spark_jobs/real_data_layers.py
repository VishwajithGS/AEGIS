"""Real-data gold layers and cross-source validation (Spark).

Runs after the batch ETL on a real-data root (see scripts/run_real_data.ps1):

* ``gold/real_daily_rainfall`` — per zone and IMD day: IMD gridded observed
  rainfall (+ IMD intensity category) and ERA5 reanalysis summed over the *same*
  24 h window (03:00 UTC → 03:00 UTC, i.e. 08:30 IST), with the difference.
* ``gold/station_daily_rainfall`` — NOAA GHCN-Daily Minambakkam gauge vs the IMD
  cell and ERA5 for the gauge's zone.
* ``gold/official_events`` — DFO and GDACS events (regional; never incidents).
* ``reports/real_data_validation.json`` — correlation, bias, MAE and category
  agreement between the sources. These describe how the *sources* agree; they
  are not a model evaluation.

    python -m spark_jobs.real_data_layers --input data_real/raw/historical
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aegis.config import data_path, get_config
from aegis.logging_config import setup_logging
from aegis.realdata.daily_rainfall import IMD_CATEGORIES
from spark_jobs.common import Timer, build_spark, lake, path_exists, reports_dir, write_json
from spark_jobs.etl_pipeline import write_table

log = setup_logging("real-data-layers")

DAILY_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("event_id", T.StringType()), ("record_type", T.StringType()), ("schema_version", T.StringType()),
    ("synthetic", T.BooleanType()), ("source_id", T.StringType()), ("zone_id", T.StringType()),
    ("date", T.StringType()), ("window_start", T.StringType()), ("window_end", T.StringType()),
    ("lat", T.DoubleType()), ("lon", T.DoubleType()), ("rainfall_mm_day", T.DoubleType()),
    ("imd_category", T.StringType()), ("data_source", T.StringType()), ("observation_type", T.StringType()),
    ("grid_lat", T.DoubleType()), ("grid_lon", T.DoubleType()), ("cell_distance_km", T.DoubleType()),
    ("station_name", T.StringType()), ("quality_flag", T.StringType()), ("source_flag", T.StringType()),
    ("licence", T.StringType()), ("retrieved_at", T.StringType())]])
EVENT_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("event_id", T.StringType()), ("source_event_id", T.StringType()), ("event_kind", T.StringType()),
    ("name", T.StringType()), ("began", T.StringType()), ("ended", T.StringType()),
    ("severity_class", T.DoubleType()), ("severity_label", T.StringType()), ("alert_level", T.StringType()),
    ("dead", T.LongType()), ("displaced", T.LongType()), ("main_cause", T.StringType()),
    ("country", T.StringType()), ("affected_area_km2", T.DoubleType()), ("centroid_lat", T.DoubleType()),
    ("centroid_lon", T.DoubleType()), ("distance_to_region_km", T.DoubleType()), ("glide", T.StringType()),
    ("spatial_scope", T.StringType()), ("intersects_study_box", T.BooleanType()),
    ("data_source", T.StringType()), ("observation_type", T.StringType()),
    ("licence", T.StringType()), ("synthetic", T.BooleanType())]])


def category_expr(col: str):
    """IMD 24-h intensity class as a Spark expression (same thresholds as aegis.realdata)."""
    expr = None
    for upper, name in IMD_CATEGORIES:
        cond = F.col(col) < F.lit(upper) if math.isfinite(upper) else F.lit(True)
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    return F.when(F.col(col).isNull(), F.lit(None).cast("string")).otherwise(expr)


def era5_daily(weather: DataFrame) -> DataFrame:
    """Sum hour-ending ERA5 values into IMD days under both possible conventions
    (IMD's day attribution is not confirmed, see aegis.realdata.daily_rainfall):

    * ``era5_mm`` (assumed): hour ending T belongs to D when D 03:00 < T <= D+1 03:00 UTC
    * ``era5_mm_alt``: window ending at 03:00 UTC on D, i.e. D-1 03:00 < T <= D 03:00"""
    era5 = weather.filter(F.col("data_source") == "era5-open-meteo")
    start_day = F.to_date(F.col("event_time") - F.expr("INTERVAL 3 HOURS") - F.expr("INTERVAL 1 SECOND"))
    a = (era5.withColumn("date", start_day).groupBy("zone_id", "date")
         .agg(F.round(F.sum("rainfall_mm_hr"), 2).alias("era5_mm"), F.count("rainfall_mm_hr").alias("era5_hours")))
    b = (era5.withColumn("date", F.date_add(start_day, 1)).groupBy("zone_id", "date")
         .agg(F.round(F.sum("rainfall_mm_hr"), 2).alias("era5_mm_alt"), F.count("rainfall_mm_hr").alias("era5_hours_alt")))
    return a.join(b, ["zone_id", "date"], "full")


def build(spark: SparkSession, cfg: dict, input_dir: Path) -> dict:
    timer = Timer()
    rf_path, ev_path = input_dir / "rainfall_daily.jsonl", input_dir / "official_events.jsonl"
    if not rf_path.exists():
        raise SystemExit(f"{rf_path} not found; run python -m aegis.realdata.ingest first")
    daily = spark.read.schema(DAILY_SCHEMA).json(str(rf_path)).withColumn("date", F.to_date("date"))
    weather_path = lake(cfg, "silver", "weather")
    if not path_exists(weather_path):
        raise SystemExit(f"{weather_path} not found; run the batch ETL on the real-data root first")
    e5 = era5_daily(spark.read.parquet(weather_path))

    imd = daily.filter(F.col("data_source") == "imd-gridded-0.25").select(
        "zone_id", "date", F.col("rainfall_mm_day").alias("imd_mm"), F.col("imd_category"),
        F.col("source_id").alias("imd_cell"), "cell_distance_km")
    zone_daily = (imd.join(e5, ["zone_id", "date"], "left")
                  .withColumn("era5_category", category_expr("era5_mm"))
                  .withColumn("era5_minus_imd_mm", F.round(F.col("era5_mm") - F.col("imd_mm"), 2))
                  .withColumn("era5_complete_day", F.col("era5_hours") == 24))
    n_zone_daily = write_table(zone_daily, lake(cfg, "gold", "real_daily_rainfall"))

    gauge = daily.filter(F.col("data_source") == "noaa-ghcnd").select(
        "zone_id", "date", F.col("source_id").alias("station_id"), "station_name",
        F.col("rainfall_mm_day").alias("gauge_mm"), F.col("imd_category").alias("gauge_category"), "quality_flag")
    station = (gauge.join(imd.select("zone_id", "date", "imd_mm", "imd_cell"), ["zone_id", "date"], "left")
               .join(e5, ["zone_id", "date"], "left"))
    n_station = write_table(station, lake(cfg, "gold", "station_daily_rainfall"))

    n_events = 0
    if ev_path.exists():
        events = spark.read.schema(EVENT_SCHEMA).json(str(ev_path))
        n_events = write_table(events, lake(cfg, "gold", "official_events"))

    report = validation_report(spark.read.parquet(lake(cfg, "gold", "real_daily_rainfall")).toPandas(),
                               spark.read.parquet(lake(cfg, "gold", "station_daily_rainfall")).toPandas())
    report.update({"rows": {"real_daily_rainfall": n_zone_daily, "station_daily_rainfall": n_station,
                            "official_events": n_events}, "duration_s": timer.seconds(),
                   "note": "Agreement between data sources, not a model evaluation. IMD = gridded gauge "
                           "observations (official); ERA5 = reanalysis; GHCN-D = single gauge."})
    write_json(reports_dir(cfg) / "real_data_validation.json", report)
    return report


def _stats(a, b) -> dict:
    import numpy as np
    m = np.isfinite(a) & np.isfinite(b)
    a, b = a[m], b[m]
    if len(a) < 3:
        return {"pairs": int(len(a))}
    return {"pairs": int(len(a)), "correlation": round(float(np.corrcoef(a, b)[0, 1]), 3),
            "mean_difference_mm": round(float(np.mean(b - a)), 2), "mae_mm": round(float(np.mean(np.abs(b - a))), 2),
            "total_a_mm": round(float(a.sum()), 1), "total_b_mm": round(float(b.sum()), 1)}


def validation_report(z, st) -> dict:
    import numpy as np
    out: dict = {}
    if len(z):
        z = z.sort_values(["zone_id", "date"])
        out["day_convention"] = ("IMD day attribution not confirmed; 'era5_*' uses the assumed window starting 08:30 IST "
                                 "on the date, '*_alt' the window ending 08:30 IST on the date")
        out["era5_vs_imd_zone_days"] = {"a": "IMD gridded", "b": "ERA5", **_stats(z["imd_mm"].to_numpy(float), z["era5_mm"].to_numpy(float))}
        out["era5_vs_imd_zone_days_alt_window"] = {"a": "IMD gridded", "b": "ERA5 (window ending on the date)",
                                                   **_stats(z["imd_mm"].to_numpy(float), z["era5_mm_alt"].to_numpy(float))}
        reg = z.groupby("date")[["imd_mm", "era5_mm"]].mean()
        out["era5_vs_imd_region_daily_mean"] = {"a": "IMD gridded", "b": "ERA5",
                                                **_stats(reg["imd_mm"].to_numpy(float), reg["era5_mm"].to_numpy(float))}
        both = z.dropna(subset=["imd_category", "era5_category"])
        out["category_agreement_exact"] = round(float((both["imd_category"] == both["era5_category"]).mean()), 3) if len(both) else None
        heavy = {"heavy", "very_heavy", "extremely_heavy"}
        imd_heavy = both["imd_category"].isin(heavy)
        out["heavy_days"] = {"imd_heavy_zone_days": int(imd_heavy.sum()),
                             "era5_also_heavy": int((imd_heavy & both["era5_category"].isin(heavy)).sum()),
                             "era5_heavy_but_imd_not": int((~imd_heavy & both["era5_category"].isin(heavy)).sum())}
        top = reg.sort_values("imd_mm", ascending=False).head(5)
        out["wettest_days_region_mean"] = [{"date": str(d)[:10], "imd_mm": round(float(r.imd_mm), 1),
                                            "era5_mm": round(float(r.era5_mm), 1) if np.isfinite(r.era5_mm) else None}
                                           for d, r in top.iterrows()]
        out["imd_cells_used"] = sorted(z["imd_cell"].dropna().unique().tolist())
        out["max_imd_cell_distance_km"] = round(float(z["cell_distance_km"].max()), 2)
    if len(st):
        out["gauge_vs_imd_cell"] = {"a": "GHCN-D gauge", "b": "IMD gridded cell",
                                    **_stats(st["gauge_mm"].to_numpy(float), st["imd_mm"].to_numpy(float))}
        out["gauge_vs_era5"] = {"a": "GHCN-D gauge", "b": "ERA5",
                                **_stats(st["gauge_mm"].to_numpy(float), st["era5_mm"].to_numpy(float))}
        out["gauge_vs_era5_alt_window"] = {"a": "GHCN-D gauge", "b": "ERA5 (window ending on the date)",
                                           **_stats(st["gauge_mm"].to_numpy(float), st["era5_mm_alt"].to_numpy(float))}
        out["gauge_days_missing"] = int(st["gauge_mm"].isna().sum())
    return out


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="Real-data gold layers and cross-source validation")
    # <data_root>/raw/historical, where aegis.realdata.ingest writes (data_root = parent of generated_dir)
    ap.add_argument("--input", default=str(data_path(cfg, "generated_dir").parent / "raw" / "historical"))
    args = ap.parse_args()
    spark = build_spark("real-data-layers", cfg)
    try:
        rep = build(spark, cfg, Path(args.input))
        print(json.dumps(rep, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
