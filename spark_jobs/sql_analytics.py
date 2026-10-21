"""Spark SQL analytics over the AEGIS lake.

Registers temporary views on the silver/gold Parquet tables and runs real SQL
(joins, CTEs, window functions, grouping). Every printed table is produced by
Spark at run time; results are also saved to ``data/reports/sql/``.

    python -m spark_jobs.sql_analytics
"""
from __future__ import annotations

import json
from pathlib import Path

from aegis.config import get_config
from spark_jobs.common import DOMAINS, Timer, build_spark, lake, path_exists, reports_dir, write_json

QUERIES: list[tuple[str, str, str]] = [
    ("q1_incidents_by_category",
     "Incident counts by category (archived incidents vs public reports)",
     """
     SELECT category,
            SUM(CASE WHEN source_type = 'historical_incident' THEN 1 ELSE 0 END) AS archived_incidents,
            SUM(CASE WHEN source_type = 'public_report' THEN 1 ELSE 0 END)       AS public_reports,
            COUNT(*)                                                           AS total
     FROM incidents
     GROUP BY category
     ORDER BY total DESC
     """),
    ("q2_incidents_by_zone",
     "Incident counts by geographic zone, with zone centre (join to the zone grid)",
     """
     SELECT i.zone_id, ROUND(z.center_lat, 4) AS center_lat, ROUND(z.center_lon, 4) AS center_lon,
            COUNT(*) AS incidents,
            SUM(CASE WHEN i.is_flood_related THEN 1 ELSE 0 END) AS flood_related,
            ROUND(100.0 * SUM(CASE WHEN i.verification_status = 'verified' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_verified
     FROM incidents i JOIN zones z ON i.zone_id = z.zone_id
     WHERE i.verification_status <> 'rejected'
     GROUP BY i.zone_id, z.center_lat, z.center_lon
     ORDER BY incidents DESC
     LIMIT 10
     """),
    ("q3_rainfall_6h_windows",
     "Average / maximum rainfall per station over 6-hour event-time windows (wettest windows)",
     """
     SELECT source_id,
            window(event_time, '6 hours').start AS window_start,
            ROUND(AVG(rainfall_mm_hr), 2) AS avg_mm_hr,
            ROUND(MAX(rainfall_mm_hr), 2) AS max_mm_hr,
            COUNT(*) AS observations
     FROM weather
     GROUP BY source_id, window(event_time, '6 hours')
     ORDER BY avg_mm_hr DESC
     LIMIT 10
     """),
    ("q4_water_level_trend",
     "Water-level trend per gauge: hourly mean and change versus the previous hour (LAG window function)",
     """
     WITH hourly AS (
       SELECT source_id, river_id, date_trunc('hour', event_time) AS hour,
              AVG(level_ratio) AS level_ratio
       FROM water_level
       WHERE NOT array_contains(dq_flags, 'sensor_fault')
       GROUP BY source_id, river_id, date_trunc('hour', event_time)
     ), trend AS (
       SELECT *, level_ratio - LAG(level_ratio) OVER (PARTITION BY source_id ORDER BY hour) AS change_1h
       FROM hourly
     )
     SELECT source_id, river_id, hour, ROUND(level_ratio, 3) AS level_ratio, ROUND(change_1h, 3) AS change_1h
     FROM trend
     WHERE change_1h IS NOT NULL
     ORDER BY change_1h DESC
     LIMIT 10
     """),
    ("q5_traffic_disruption",
     "Traffic disruption summary per zone (share of disrupted observations, mean speed ratio)",
     """
     SELECT zone_id,
            COUNT(*) AS observations,
            ROUND(AVG(CASE WHEN is_disrupted THEN 1.0 ELSE 0.0 END), 3) AS disrupted_share,
            ROUND(AVG(speed_ratio), 3) AS mean_speed_ratio,
            SUM(CASE WHEN disruption_level = 'flooded' THEN 1 ELSE 0 END) AS flooded_reports
     FROM traffic
     GROUP BY zone_id
     ORDER BY disrupted_share DESC
     LIMIT 10
     """),
    ("q6_zone_risk_ranking",
     "Zone risk ranking: mean and peak rule-based risk with RANK() over the full history",
     """
     SELECT zone_id,
            ROUND(AVG(risk_score), 2) AS mean_risk,
            ROUND(MAX(risk_score), 2) AS peak_risk,
            SUM(CASE WHEN risk_category IN ('High', 'Critical') THEN 1 ELSE 0 END) AS high_or_critical_hours,
            RANK() OVER (ORDER BY AVG(risk_score) DESC) AS risk_rank
     FROM zone_risk
     WHERE risk_score IS NOT NULL
     GROUP BY zone_id
     ORDER BY risk_rank
     LIMIT 10
     """),
    ("q7_daily_incident_trend",
     "Historical incident trend by day with a 3-day moving average (window frame)",
     """
     WITH daily AS (
       SELECT event_date, COUNT(*) AS incidents
       FROM incidents
       WHERE source_type = 'historical_incident'
       GROUP BY event_date
     )
     SELECT event_date, incidents,
            ROUND(AVG(incidents) OVER (ORDER BY event_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3d
     FROM daily
     ORDER BY incidents DESC
     LIMIT 10
     """),
    ("q8_data_quality",
     "Data-quality summary: rejected records by domain and reason (from the quarantine table)",
     """
     SELECT domain, reason, COUNT(*) AS rejected
     FROM quarantine LATERAL VIEW explode(reason_codes) r AS reason
     GROUP BY domain, reason
     ORDER BY domain, rejected DESC
     """),
    ("q9_ml_label_distribution",
     "Severity-label distribution in the ML training set (synthetic labels), with feature means",
     """
     SELECT label, COUNT(*) AS rows,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
            ROUND(AVG(rain_3h_mm), 2) AS mean_rain_3h_mm,
            ROUND(AVG(level_ratio_max_3h), 3) AS mean_level_ratio,
            ROUND(AVG(reports_6h), 2) AS mean_reports_6h
     FROM ml_training
     GROUP BY label
     ORDER BY CASE label WHEN 'Low' THEN 1 WHEN 'Moderate' THEN 2 WHEN 'High' THEN 3 ELSE 4 END
     """),
    ("q10_hourly_pattern",
     "Hour-of-day (IST) pattern of flood-related incidents",
     """
     SELECT hour(from_utc_timestamp(event_time, 'Asia/Kolkata')) AS hour_ist, COUNT(*) AS flood_related
     FROM incidents WHERE is_flood_related
     GROUP BY hour(from_utc_timestamp(event_time, 'Asia/Kolkata'))
     ORDER BY flood_related DESC
     LIMIT 8
     """),
]

VIEWS = {
    "weather": ("silver", "weather"), "water_level": ("silver", "water_level"), "traffic": ("silver", "traffic"),
    "incidents": ("silver", "incidents"), "zone_risk": ("gold", "zone_risk_hourly"),
    "zone_hourly": ("gold", "zone_hourly"), "ml_training": ("gold", "ml_training_flood_severity"),
    "quarantine": ("quarantine", "batch"),
}


def main() -> None:
    cfg = get_config()
    timer = Timer()
    spark = build_spark("sql-analytics", cfg)
    missing = []
    for view, parts in VIEWS.items():
        path = lake(cfg, *parts)
        if path_exists(path):
            spark.read.parquet(path).createOrReplaceTempView(view)
        else:
            missing.append(view)
    from spark_jobs.spatial_analytics import zones_df
    zones_df(spark, cfg).createOrReplaceTempView("zones")
    if missing:
        raise SystemExit(f"missing lake tables for views {missing}; run the batch ETL first")

    out_dir = reports_dir(cfg) / "sql"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {"views": sorted(list(VIEWS) + ["zones"]), "queries": []}
    for name, title, sql in QUERIES:
        print(f"\n=== {name}: {title} ===")
        t = Timer()
        df = spark.sql(sql)
        rows = df.collect()            # every query is aggregated / LIMITed -> small
        df.show(len(rows), truncate=False)
        records = [r.asDict() for r in rows]
        (out_dir / f"{name}.json").write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
        summary["queries"].append({"name": name, "title": title, "rows": len(rows), "seconds": t.seconds(),
                                   "sql": " ".join(sql.split())})
    summary["duration_s"] = timer.seconds()
    write_json(reports_dir(cfg) / "sql_analytics.json", summary)
    print(f"\n{len(QUERIES)} Spark SQL queries executed in {summary['duration_s']} s; outputs -> {out_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
