"""Write Spark DataFrames to HBase and verify by reading back.

Each partition opens its own Thrift connection and sends batched puts, so the
write is distributed across executors. The number of rows written is counted
with an accumulator and compared with the DataFrame count; a sample of row
keys is then read back from HBase to confirm persistence.
"""
from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from aegis.storage.hbase_repository import (prediction_row_key, sensor_row_key, zone_risk_row_key,
                                            HBaseRepository)

# family -> columns. Absent/NULL values are simply not written (sparse rows).
SENSOR_SPEC = {
    "measurement": ["rainfall_mm_hr", "temperature_c", "humidity_pct", "condition",
                    "water_level_m", "danger_level_m", "level_ratio", "sensor_status",
                    "avg_speed_kmph", "free_flow_speed_kmph", "density_veh_per_km", "speed_ratio",
                    "disruption_level", "is_disrupted"],
    "location": ["lat", "lon", "zone_id", "segment_id", "river_id", "station_name"],
    "metadata": ["event_id", "domain", "source_type", "source_id", "schema_version", "event_time_iso",
                 "ingest_time_iso", "dq_flags", "origin", "is_synthetic"],
}
INCIDENT_SPEC = {
    "incident": ["incident_id", "event_id", "category", "category_raw", "severity", "description", "source_type",
                 "source_id", "schema_version", "event_time_iso", "channel", "affected_road", "affected_area_km2",
                 "is_flood_related", "is_synthetic"],
    "location": ["lat", "lon", "zone_id", "has_location"],
    "verification": ["verification_status", "confidence", "report_weight", "dq_flags"],
}
ZONE_RISK_SPEC = {
    "risk": ["risk_score", "risk_category", "confidence", "coverage", "explanation", "contributions"],
    "summary": ["window_start_iso", "window_end_iso", "assessed_at_iso", "data_origin", "rainfall_avg_mm_hr",
                "rainfall_max_mm_hr", "humidity_avg", "level_ratio_max", "disrupted_ratio", "speed_ratio_avg",
                "weighted_reports", "reports", "flood_reports", "historical_hazard_index"],
    "quality": ["weather_obs", "water_obs", "traffic_obs", "signal_count", "available_features", "missing_features"],
}
PREDICTION_SPEC = {
    "input": ["rain_3h_mm", "rain_max_1h_mm_hr", "level_ratio_max_3h", "humidity_avg_3h", "disrupted_ratio_1h",
              "speed_ratio_avg_1h", "reports_6h", "water_obs_available", "traffic_obs_available", "input_source"],
    "output": ["predicted_severity", "probabilities", "prediction_status"],
    "meta": ["prediction_id", "zone_id", "model_version", "label_provenance", "predicted_at_iso", "features_as_of_iso"],
}


def sensor_key(d: dict) -> str:
    return sensor_row_key(d["source_id"], int(d["event_ts_ms"]), d["event_id"])


def incident_key(d: dict) -> str:
    return str(d["incident_id"])


def zone_risk_key(d: dict) -> str:
    return zone_risk_row_key(d["zone_id"], int(d["window_start_ms"]), d.get("risk_series") or "live")


def prediction_key(d: dict) -> str:
    return prediction_row_key(d["zone_id"] or "api", int(d["predicted_at_ms"]), d["prediction_id"])


def iso(col: str):
    return F.date_format(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")


def prepare_sensor(df: DataFrame) -> DataFrame:
    return (df.withColumn("event_ts_ms", F.expr("unix_millis(event_time)"))
              .withColumn("event_time_iso", iso("event_time"))
              .withColumn("ingest_time_iso", iso("ingest_time"))
              .withColumn("dq_flags", F.to_json("dq_flags")))


def prepare_incident(df: DataFrame) -> DataFrame:
    return df.withColumn("event_time_iso", iso("event_time")).withColumn("dq_flags", F.to_json("dq_flags"))


def prepare_zone_risk(df: DataFrame) -> DataFrame:
    return (df.withColumn("window_start_ms", F.expr("unix_millis(window_start)"))
              .withColumn("window_start_iso", iso("window_start"))
              .withColumn("window_end_iso", iso("window_end"))
              .withColumn("assessed_at_iso", iso("assessed_at"))
              .withColumn("available_features", F.to_json("available_features"))
              .withColumn("missing_features", F.to_json("missing_features")))


def write(df: DataFrame, table: str, spec: dict, key_fn: Callable[[dict], str], hbase_cfg: dict) -> int:
    """Distributed batched puts; returns rows written (accumulator)."""
    acc = df.sparkSession.sparkContext.accumulator(0)
    cols = sorted({c for cs in spec.values() for c in cs} | {"source_id", "event_id", "event_ts_ms", "zone_id",
                                                              "window_start_ms", "risk_series", "incident_id", "prediction_id",
                                                              "predicted_at_ms"})
    present = [c for c in cols if c in df.columns]
    params = {k: hbase_cfg[k] for k in ("host", "port", "timeout_ms", "batch_size", "write_retries", "retry_backoff_s")}

    def _partition(rows) -> None:
        repo = HBaseRepository(**params)
        buf: list = []
        try:
            for row in rows:
                d = row.asDict()
                buf.append((key_fn(d), {fam: {c: d.get(c) for c in cs} for fam, cs in spec.items()}))
                if len(buf) >= params["batch_size"]:
                    acc.add(repo.put_many(table, buf))
                    buf = []
            if buf:
                acc.add(repo.put_many(table, buf))
        finally:
            repo.close()

    df.select(*present).foreachPartition(_partition)
    return int(acc.value)


def verify_sample(df: DataFrame, table: str, key_fn: Callable[[dict], str], hbase_cfg: dict,
                  check_family: str, check_column: str, n: int = 3) -> dict:
    """Read back up to ``n`` written rows and compare one column."""
    sample = [r.asDict() for r in df.limit(n).collect()]
    repo = HBaseRepository.from_config({"hbase": hbase_cfg})
    found, mismatched = 0, []
    try:
        for d in sample:
            key = key_fn(d)
            got = repo.get(table, key)
            if got is None:
                mismatched.append({"row_key": key, "error": "row not found"})
                continue
            expected, actual = d.get(check_column), got.get(check_family, {}).get(check_column)
            if expected is not None and str(actual) != str(expected):
                mismatched.append({"row_key": key, "expected": expected, "actual": actual})
            else:
                found += 1
    finally:
        repo.close()
    return {"table": table, "sampled": len(sample), "verified": found, "problems": mismatched}


def hbase_settings(cfg: dict) -> Optional[dict]:
    h = cfg["hbase"]
    return dict(h) if h.get("enabled", True) else None
