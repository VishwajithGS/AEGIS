"""Geospatial analytics on Spark.

* Zone grid as a DataFrame (regular analysis grid, not official boundaries).
* Nearest-station assignment: a Spark cross join of zone centres x stations
  with a Haversine distance expression and a window rank. Each zone takes its
  rainfall from the nearest weather station within
  ``region.weather_station_radius_km`` and its water level from the nearest
  river gauge within ``region.water_station_radius_km``; zones outside both
  radii have no observation for that indicator (reported as missing).
* Nearby-hazard search within a radius (km) of a point.
* Incident density and zone comparisons.

    python -m spark_jobs.spatial_analytics --lat 13.05 --lon 80.22 --radius 3
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from aegis.config import data_path, get_config
from aegis.geo import ZoneGrid
from aegis.logging_config import setup_logging
from spark_jobs.common import build_spark, lake, read_parquet_if_exists, reports_dir, write_json
from spark_jobs.transforms import haversine_km_col

log = setup_logging("spatial")


def zones_df(spark: SparkSession, cfg: dict) -> DataFrame:
    rows = [(z.zone_id, z.row, z.col, z.min_lat, z.min_lon, z.max_lat, z.max_lon, z.center[0], z.center[1])
            for z in ZoneGrid.from_config(cfg).zones()]
    return spark.createDataFrame(rows, "zone_id string, row int, col int, min_lat double, min_lon double, "
                                       "max_lat double, max_lon double, center_lat double, center_lon double")


def load_station_registry(cfg: dict, path: Optional[Path] = None) -> Optional[dict]:
    p = path or data_path(cfg, "reference_dir", "stations.json")
    if not Path(p).exists():
        log.warning(f"station registry not found at {p}; stations will only cover their own zone")
        return None
    return json.loads(Path(p).read_text(encoding="utf-8"))


def stations_df(spark: SparkSession, registry: dict, kind: str) -> DataFrame:
    key = {"weather": "weather_stations", "water_level": "water_stations"}[kind]
    rows = [(s["station_id"], float(s["lat"]), float(s["lon"])) for s in registry[key]]
    return spark.createDataFrame(rows, "station_id string, station_lat double, station_lon double")


def nearest_station_map(spark: SparkSession, cfg: dict, kind: str,
                        registry: Optional[dict] = None) -> Optional[DataFrame]:
    """zone_id -> nearest station within the configured radius (Spark spatial join)."""
    registry = registry if registry is not None else load_station_registry(cfg)
    if not registry:
        return None
    radius = float(cfg["region"]["weather_station_radius_km" if kind == "weather" else "water_station_radius_km"])
    pairs = (zones_df(spark, cfg).crossJoin(F.broadcast(stations_df(spark, registry, kind)))
             .withColumn("distance_km", haversine_km_col(F.col("center_lat"), F.col("center_lon"),
                                                         F.col("station_lat"), F.col("station_lon"))))
    w = Window.partitionBy("zone_id").orderBy("distance_km", "station_id")
    return (pairs.withColumn("rank", F.row_number().over(w))
            .filter((F.col("rank") == 1) & (F.col("distance_km") <= radius))
            .select("zone_id", F.col("station_id"), F.round("distance_km", 3).alias("distance_km")))


def nearby(df: DataFrame, lat: float, lon: float, radius_km: float) -> DataFrame:
    """Rows of ``df`` (with lat/lon) within ``radius_km`` of the point, nearest first."""
    d = haversine_km_col(F.lit(lat), F.lit(lon), F.col("lat"), F.col("lon"))
    return (df.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
            .withColumn("distance_km", F.round(d, 3))
            .filter(F.col("distance_km") <= radius_km)
            .orderBy("distance_km"))


def incident_density(incidents: DataFrame, zones: DataFrame) -> DataFrame:
    """Incidents per zone and per km^2 (cell area from the Haversine extent)."""
    area = (haversine_km_col(F.col("min_lat"), F.col("min_lon"), F.col("max_lat"), F.col("min_lon"))
            * haversine_km_col(F.col("center_lat"), F.col("min_lon"), F.col("center_lat"), F.col("max_lon")))
    counts = (incidents.filter(F.col("zone_id").isNotNull()).groupBy("zone_id")
              .agg(F.count(F.lit(1)).alias("incidents"),
                   F.sum(F.col("is_flood_related").cast("int")).alias("flood_related")))
    return (zones.withColumn("area_km2", F.round(area, 2)).join(counts, "zone_id", "left")
            .fillna({"incidents": 0, "flood_related": 0})
            .withColumn("incidents_per_km2", F.round(F.col("incidents") / F.col("area_km2"), 3))
            .orderBy(F.desc("incidents_per_km2")))


def main() -> None:
    ap = argparse.ArgumentParser(description="AEGIS spatial analytics demonstration")
    ap.add_argument("--lat", type=float, default=13.05)
    ap.add_argument("--lon", type=float, default=80.22)
    ap.add_argument("--radius", type=float, default=3.0, help="search radius in km")
    args = ap.parse_args()
    cfg = get_config()
    spark = build_spark("spatial", cfg)
    report: dict = {"query_point": {"lat": args.lat, "lon": args.lon, "radius_km": args.radius}}

    zones = zones_df(spark, cfg)
    wmap = nearest_station_map(spark, cfg, "weather")
    lmap = nearest_station_map(spark, cfg, "water_level")
    print("\n=== Zone -> nearest weather station (Haversine, Spark cross join + window rank) ===")
    if wmap is not None:
        wmap.orderBy("zone_id").show(10, truncate=False)
        report["zones_with_weather_station"] = wmap.count()
    if lmap is not None:
        report["zones_with_water_gauge"] = lmap.count()
        print(f"zones covered by a river gauge within {cfg['region']['water_station_radius_km']} km: "
              f"{report['zones_with_water_gauge']} of {zones.count()}")

    incidents = read_parquet_if_exists(spark, lake(cfg, "silver", "incidents"))
    if incidents is None:
        print("silver incidents not found - run the batch ETL first (scripts/run_batch_analytics.ps1)")
    else:
        near = nearby(incidents, args.lat, args.lon, args.radius)
        print(f"\n=== Hazards within {args.radius} km of ({args.lat}, {args.lon}) ===")
        near.select("incident_id", "source_type", "category", "severity", "verification_status",
                    "event_time", "distance_km").show(15, truncate=False)
        report["nearby_count"] = near.count()
        report["nearby_by_category"] = {r["category"]: r["count"] for r in near.groupBy("category").count().collect()}
        dens = incident_density(incidents, zones)
        print("\n=== Incident density by zone (top 10) ===")
        dens.select("zone_id", "incidents", "flood_related", "area_km2", "incidents_per_km2").show(10)
        report["top_density_zones"] = [r.asDict() for r in
                                       dens.select("zone_id", "incidents", "incidents_per_km2").limit(5).collect()]

    risk = read_parquet_if_exists(spark, lake(cfg, "gold", "zone_risk_hourly"))
    if risk is not None:
        last_day = risk.agg(F.max("hour")).first()[0]
        cmp = (risk.withColumn("period", F.when(F.col("hour") > F.lit(last_day) - F.expr("INTERVAL 24 HOURS"),
                                                F.lit("last_24h"))
                               .when(F.col("hour") > F.lit(last_day) - F.expr("INTERVAL 48 HOURS"), F.lit("prev_24h")))
               .filter(F.col("period").isNotNull())
               .groupBy("zone_id").pivot("period", ["prev_24h", "last_24h"]).agg(F.round(F.avg("risk_score"), 2))
               .withColumn("change", F.round(F.col("last_24h") - F.col("prev_24h"), 2))
               .orderBy(F.desc_nulls_last("change")))
        print("\n=== Time-based comparison of mean zone risk (last 24h vs previous 24h) ===")
        cmp.show(10)
        report["risk_change_top"] = [r.asDict() for r in cmp.limit(5).collect()]
    write_json(reports_dir(cfg) / "spatial_report.json", report)
    print(f"\nreport written to {reports_dir(cfg) / 'spatial_report.json'}")
    spark.stop()


if __name__ == "__main__":
    main()
