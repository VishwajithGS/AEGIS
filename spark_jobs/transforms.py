"""Source-to-canonical mapping, validation and data-quality flags (Spark).

Used unchanged by the batch ETL (``etl_pipeline.py``) and by Structured
Streaming (``streaming_pipeline.py``) so both layers apply identical rules.

Input contract - a DataFrame of raw records with columns::

    value STRING            raw JSON payload exactly as received
    topic STRING            Kafka topic (or the topic the file stands in for)
    kafka_partition INT     NULL for file sources
    kafka_offset BIGINT     NULL for file sources
    ingest_time TIMESTAMP   Kafka record timestamp; NULL for file sources
    origin STRING           'kafka' | 'file:<name>'
    source_seq BIGINT       arrival order within the origin (offset or line number)

Every raw JSON leaf is parsed as STRING and cast explicitly, so a wrong type
becomes ``invalid_type:<field>`` instead of making the whole record unreadable.
Only syntactically broken JSON is ``malformed_json``.
"""
from __future__ import annotations

from functools import reduce
from typing import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aegis import canonical as C
from aegis.geo import ZoneGrid

S = T.StringType()
CORRUPT = "_corrupt_record"


def _fields(*names: str) -> list:
    return [T.StructField(n, S) for n in names]


RAW_SCHEMAS = {
    "weather": T.StructType(
        _fields("event_id", "obs_id", "source_type", "source_id", "station", "schema_version", "synthetic",
                "event_time", "ts_ms", "lat", "lon")
        + [T.StructField("location", T.StructType(_fields("latitude", "longitude")))]
        + _fields("temperature_c", "humidity_pct", "rainfall_mm_hr", "condition",
                  "temp_f", "rh_pct", "precip_in_hr", "sky", "data_source", "observation_type", CORRUPT)),
    "water_level": T.StructType(_fields(
        "event_id", "source_type", "source_id", "schema_version", "synthetic", "event_time", "lat", "lon",
        "river_id", "station_name", "water_level", "unit", "danger_level", "danger_level_unit",
        "sensor_status", "data_source", "observation_type", CORRUPT)),
    "traffic": T.StructType(_fields(
        "event_id", "source_type", "source_id", "schema_version", "synthetic", "event_time", "segment_id",
        "lat", "lon", "vehicle_density", "avg_speed_kmph", "free_flow_speed_kmph", "disruption", "data_source",
        "observation_type", CORRUPT)),
    "incidents": T.StructType(_fields(
        "event_id", "source_type", "source_id", "schema_version", "synthetic", "event_time", "report_id",
        "incident_id", "lat", "lon", "category", "severity", "description", "channel",
        "verification_status", "confidence", "affected_road", "affected_area_km2", "data_source",
        "observation_type", CORRUPT)),
}

COMMON_COLUMNS = [
    "domain", "event_id", "source_type", "source_id", "schema_version", "event_time", "event_date",
    "lat", "lon", "zone_id", "is_synthetic", "data_source", "observation_type", "dq_flags", "reasons", "is_valid",
    "topic", "origin", "kafka_partition", "kafka_offset", "source_seq", "ingest_time", "processing_time", "raw_value",
]
DOMAIN_COLUMNS = {
    "weather": ["temperature_c", "humidity_pct", "rainfall_mm_hr", "condition", "condition_raw"],
    "water_level": ["river_id", "station_name", "water_level_m", "danger_level_m", "level_ratio",
                    "sensor_status", "unit_raw"],
    "traffic": ["segment_id", "density_veh_per_km", "avg_speed_kmph", "free_flow_speed_kmph", "speed_ratio",
                "disruption_level", "is_disrupted"],
    "incidents": ["incident_id", "category", "category_raw", "severity", "description", "channel",
                  "verification_status", "confidence", "report_weight", "is_flood_related",
                  "affected_road", "affected_area_km2", "has_location"],
}
# Columns persisted in silver tables (raw payload and Kafka bookkeeping excluded).
SILVER_COMMON = ["event_id", "source_type", "source_id", "schema_version", "event_time", "event_date",
                 "lat", "lon", "zone_id", "is_synthetic", "data_source", "observation_type", "dq_flags", "origin",
                 "ingest_time", "processing_time"]


def topic_domains(cfg: dict) -> dict[str, str]:
    t = cfg["kafka"]["topics"]
    return {t["weather"]: "weather", t["water_level"]: "water_level", t["traffic"]: "traffic",
            t["incidents"]: "incidents"}


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------
def _lookup(col: Column, mapping: dict) -> Column:
    """Normalised lookup: lower(trim(col)) -> canonical value, NULL if unknown."""
    m = F.create_map(*[F.lit(x) for kv in sorted(mapping.items()) for x in kv])
    return m[F.lower(F.trim(col))]


def _num(col: Column) -> Column:
    return col.cast("double")


def _reason(cond: Column, code: str) -> Column:
    return F.when(cond, F.lit(code))


def _non_null(cols: list) -> Column:
    return F.filter(F.array(*cols), lambda x: x.isNotNull())


def zone_id_col(lat: Column, lon: Column, grid: ZoneGrid) -> Column:
    """Spark twin of ``ZoneGrid.zone_for`` (floor of the division, clamped at the
    north/east edge; NULL outside the bounding box)."""
    row = F.least(F.floor((lat - F.lit(grid.min_lat)) / F.lit(grid.cell)), F.lit(grid.rows - 1)).cast("int")
    col = F.least(F.floor((lon - F.lit(grid.min_lon)) / F.lit(grid.cell)), F.lit(grid.cols - 1)).cast("int")
    inside = lat.between(grid.min_lat, grid.max_lat) & lon.between(grid.min_lon, grid.max_lon)
    return F.when(inside, F.format_string("Z-R%02d-C%02d", row, col))


def haversine_km_col(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance (km) as a Spark expression; matches aegis.geo.haversine_km."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dp, dl = p2 - p1, F.radians(lon2) - F.radians(lon1)
    a = F.pow(F.sin(dp / 2), 2) + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dl / 2), 2)
    return F.lit(2 * 6371.0088) * F.asin(F.least(F.lit(1.0), F.sqrt(a)))


def _range_reasons(values: dict[str, Column]) -> list:
    out = []
    for name, v in values.items():
        lo, hi = C.RANGES[name]
        out.append(_reason(v.isNotNull() & ((v < lo) | (v > hi)), f"out_of_range:{name}"))
    return out


# ---------------------------------------------------------------------------
# Canonicalisation
# ---------------------------------------------------------------------------
def parse(df_raw: DataFrame, domain: str) -> DataFrame:
    return df_raw.withColumn("j", F.from_json(F.col("value"), RAW_SCHEMAS[domain],
                                              {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT}))


def _common_checks(event_id_raw: Column, source_id_raw: Column, time_raw: Column, ts: Column,
                   lat_raw: Column, lon_raw: Column, lat: Column, lon: Column, coords_required: bool,
                   grid: ZoneGrid, future_tolerance_min: int) -> list:
    checks = [
        _reason(event_id_raw.isNull() | (F.trim(event_id_raw) == ""), "missing_required:event_id"),
        _reason(source_id_raw.isNull(), "missing_required:source_id"),
        _reason(time_raw.isNull(), "missing_required:event_time"),
        _reason(time_raw.isNotNull() & ts.isNull(), "invalid_timestamp"),
        _reason(ts > F.current_timestamp() + F.expr(f"INTERVAL {int(future_tolerance_min)} MINUTES"),
                "future_timestamp"),
        _reason(lat_raw.isNotNull() & lat.isNull(), "invalid_type:lat"),
        _reason(lon_raw.isNotNull() & lon.isNull(), "invalid_type:lon"),
    ]
    valid_coord = lat.between(-90.0, 90.0) & lon.between(-180.0, 180.0)
    checks.append(_reason(lat.isNotNull() & lon.isNotNull() & ~valid_coord, "invalid_coordinates"))
    inside = lat.between(grid.min_lat, grid.max_lat) & lon.between(grid.min_lon, grid.max_lon)
    checks.append(_reason(lat.isNotNull() & lon.isNotNull() & valid_coord & ~inside, "outside_study_region"))
    if coords_required:
        checks.append(_reason(lat_raw.isNull(), "missing_required:lat"))
        checks.append(_reason(lon_raw.isNull(), "missing_required:lon"))
    return checks


def canonicalize(df_raw: DataFrame, domain: str, cfg: dict) -> DataFrame:
    """Parse, map to the canonical schema, validate and flag one domain."""
    grid = ZoneGrid.from_config(cfg)
    tol = int(cfg["streaming"]["future_tolerance_minutes"])
    wm_seconds = _interval_seconds(cfg["streaming"]["watermark"])
    j = F.col("j")
    df = parse(df_raw, domain)
    version = j["schema_version"]
    malformed = j.isNull() | j[CORRUPT].isNotNull()
    expected_types = C.TOPIC_SOURCE_TYPES[domain]
    all_types = [t for ts_ in C.TOPIC_SOURCE_TYPES.values() for t in ts_]
    known_versions = [v for t in expected_types for v in C.SCHEMA_VERSIONS[t]]

    if domain == "weather":
        imp = version == "wx-imperial-2"
        event_id_raw = F.when(imp, j["obs_id"]).otherwise(j["event_id"])
        source_id_raw = F.when(imp, j["station"]).otherwise(j["source_id"])
        time_raw = F.when(imp, j["ts_ms"]).otherwise(j["event_time"])
        ts = F.when(imp, F.expr("timestamp_millis(cast(j.ts_ms as bigint))")).otherwise(j["event_time"].cast("timestamp"))
        lat_raw = F.when(imp, j["location"]["latitude"]).otherwise(j["lat"])
        lon_raw = F.when(imp, j["location"]["longitude"]).otherwise(j["lon"])
        temp_raw = F.when(imp, j["temp_f"]).otherwise(j["temperature_c"])
        hum_raw = F.when(imp, j["rh_pct"]).otherwise(j["humidity_pct"])
        rain_raw = F.when(imp, j["precip_in_hr"]).otherwise(j["rainfall_mm_hr"])
        cond_raw = F.when(imp, j["sky"]).otherwise(j["condition"])
        temp = F.when(imp, (_num(temp_raw) - 32.0) * 5.0 / 9.0).otherwise(_num(temp_raw))
        hum = _num(hum_raw)
        rain = F.when(imp, _num(rain_raw) * C.INCH_TO_MM).otherwise(_num(rain_raw))
        condition = _lookup(cond_raw, C.WEATHER_CONDITIONS)
        extra = {
            "temperature_c": F.round(temp, 2), "humidity_pct": F.round(hum, 1),
            "rainfall_mm_hr": F.round(rain, 3), "condition": F.coalesce(condition, F.lit("unknown")),
            "condition_raw": cond_raw,
        }
        checks = [
            _reason(rain_raw.isNull(), "missing_required:rainfall"),
            _reason(rain_raw.isNotNull() & rain.isNull(), "invalid_type:rainfall"),
            _reason(temp_raw.isNotNull() & temp.isNull(), "invalid_type:temperature"),
            _reason(hum_raw.isNotNull() & hum.isNull(), "invalid_type:humidity"),
        ] + _range_reasons({"rainfall_mm_hr": rain, "temperature_c": temp, "humidity_pct": hum})
        flags = [_reason(cond_raw.isNotNull() & condition.isNull(), "unknown_condition"),
                 _reason(temp_raw.isNull() | hum_raw.isNull(), "missing_optional")]
        coords_required = True

    elif domain == "water_level":
        event_id_raw, source_id_raw, time_raw = j["event_id"], j["source_id"], j["event_time"]
        ts = j["event_time"].cast("timestamp")
        lat_raw, lon_raw = j["lat"], j["lon"]
        factor = _lookup(j["unit"], C.LENGTH_TO_METRES)
        dfactor = F.coalesce(_lookup(j["danger_level_unit"], C.LENGTH_TO_METRES), F.lit(1.0))
        level = _num(j["water_level"]) * factor
        danger = _num(j["danger_level"]) * dfactor
        status = _lookup(j["sensor_status"], C.SENSOR_STATUS)
        extra = {
            "river_id": j["river_id"], "station_name": j["station_name"],
            "water_level_m": F.round(level, 4), "danger_level_m": F.round(danger, 4),
            "level_ratio": F.round(F.when(danger > 0, level / danger), 4),
            "sensor_status": F.coalesce(status, F.lit("unknown")), "unit_raw": j["unit"],
        }
        checks = [
            _reason(j["water_level"].isNull(), "missing_required:water_level"),
            _reason(j["unit"].isNull(), "missing_required:unit"),
            _reason(j["water_level"].isNotNull() & _num(j["water_level"]).isNull(), "invalid_type:water_level"),
            _reason(j["unit"].isNotNull() & factor.isNull(), "invalid_unit"),
            _reason(j["danger_level"].isNotNull() & _num(j["danger_level"]).isNull(), "invalid_type:danger_level"),
        ] + _range_reasons({"water_level_m": level, "danger_level_m": danger})
        flags = [_reason(status == "degraded", "sensor_degraded"), _reason(status == "fault", "sensor_fault"),
                 _reason(j["sensor_status"].isNotNull() & status.isNull(), "unknown_status"),
                 _reason(j["danger_level"].isNull(), "missing_optional")]
        coords_required = True

    elif domain == "traffic":
        event_id_raw, source_id_raw, time_raw = j["event_id"], j["source_id"], j["event_time"]
        local_tz = cfg["region"]["local_timezone"]
        ts = F.coalesce(
            F.to_utc_timestamp(F.to_timestamp(j["event_time"], "yyyy-MM-dd HH:mm:ss"), local_tz),
            j["event_time"].cast("timestamp"))
        lat_raw, lon_raw = j["lat"], j["lon"]
        speed, ff, dens = _num(j["avg_speed_kmph"]), _num(j["free_flow_speed_kmph"]), _num(j["vehicle_density"])
        level = _lookup(j["disruption"], C.TRAFFIC_DISRUPTION)
        ratio = F.when(ff > 0, speed / ff)
        extra = {
            "segment_id": j["segment_id"], "density_veh_per_km": F.round(dens, 2),
            "avg_speed_kmph": F.round(speed, 2), "free_flow_speed_kmph": ff,
            "speed_ratio": F.round(ratio, 4), "disruption_level": F.coalesce(level, F.lit("unknown")),
            "is_disrupted": (F.coalesce(level, F.lit("")).isin(*C.DISRUPTED_LEVELS)
                             | (F.coalesce(ratio, F.lit(1.0)) < C.DISRUPTED_SPEED_RATIO)),
        }
        checks = [
            _reason(j["avg_speed_kmph"].isNull(), "missing_required:avg_speed_kmph"),
            _reason(j["avg_speed_kmph"].isNotNull() & speed.isNull(), "invalid_type:avg_speed_kmph"),
            _reason(j["vehicle_density"].isNotNull() & dens.isNull(), "invalid_type:vehicle_density"),
            _reason(j["free_flow_speed_kmph"].isNotNull() & ff.isNull(), "invalid_type:free_flow_speed_kmph"),
        ] + _range_reasons({"avg_speed_kmph": speed, "density_veh_per_km": dens, "free_flow_speed_kmph": ff})
        flags = [_reason(j["disruption"].isNotNull() & level.isNull(), "unknown_category"),
                 _reason(j["vehicle_density"].isNull() | j["free_flow_speed_kmph"].isNull(), "missing_optional")]
        coords_required = True

    elif domain == "incidents":
        hi = version == "hi-1"
        event_id_raw, source_id_raw, time_raw = j["event_id"], j["source_id"], j["event_time"]
        ts = j["event_time"].cast("timestamp")
        lat_raw, lon_raw = j["lat"], j["lon"]
        category = _lookup(j["category"], C.INCIDENT_CATEGORIES)
        severity = _lookup(j["severity"], C.SEVERITY_NORMALIZE)
        verification = F.when(hi, F.lit("verified")).otherwise(_lookup(j["verification_status"], C.VERIFICATION_STATUS))
        conf = _num(j["confidence"])
        area = _num(j["affected_area_km2"])
        rw = cfg["risk"]["report_weights"]
        cat_final = F.coalesce(category, F.lit("other"))
        ver_final = F.coalesce(verification, F.lit("unverified"))
        extra = {
            "incident_id": F.when(hi, j["incident_id"]).otherwise(F.coalesce(j["report_id"], j["event_id"])),
            "category": cat_final, "category_raw": j["category"],
            "severity": F.when(hi, severity),           # observed severity only exists in the archive
            "description": F.substring(j["description"], 1, 500), "channel": j["channel"],
            "verification_status": ver_final,
            "confidence": conf,
            "report_weight": F.when(hi, F.lit(None).cast("double")).otherwise(
                F.when(ver_final == "verified", F.lit(float(rw["verified"])))
                 .when(ver_final == "rejected", F.lit(float(rw["rejected"])))
                 .otherwise(F.lit(float(rw["unverified"])))),
            "is_flood_related": cat_final.isin(*C.FLOOD_RELATED_CATEGORIES),
            "affected_road": j["affected_road"], "affected_area_km2": area,
            "has_location": j["lat"].isNotNull() & j["lon"].isNotNull(),
        }
        checks = [
            _reason(j["category"].isNull(), "missing_required:category"),
            _reason(hi & j["incident_id"].isNull(), "missing_required:incident_id"),
            _reason(hi & j["lat"].isNull(), "missing_required:lat"),
            _reason(hi & j["lon"].isNull(), "missing_required:lon"),
            _reason(j["confidence"].isNotNull() & conf.isNull(), "invalid_type:confidence"),
            _reason(j["affected_area_km2"].isNotNull() & area.isNull(), "invalid_type:affected_area_km2"),
            _reason(hi & j["severity"].isNotNull() & severity.isNull(), "invalid_type:severity"),
        ] + _range_reasons({"confidence": conf, "affected_area_km2": area})
        flags = [_reason(j["category"].isNotNull() & category.isNull(), "unknown_category"),
                 _reason(~hi & (j["lat"].isNull() | j["lon"].isNull()), "missing_coordinates"),
                 _reason(~hi & j["verification_status"].isNotNull() & verification.isNull(), "unknown_status")]
        coords_required = False
    else:
        raise ValueError(f"unknown domain {domain}")

    lat, lon = _num(lat_raw), _num(lon_raw)
    checks = _common_checks(event_id_raw, source_id_raw, time_raw, ts, lat_raw, lon_raw, lat, lon,
                            coords_required, grid, tol) + checks

    source_type = j["source_type"]
    reasons = (
        F.when(malformed, F.array(F.lit("malformed_json")))
        .when(source_type.isNotNull() & source_type.isin(*all_types) & ~source_type.isin(*expected_types),
              F.array(F.lit("topic_source_mismatch")))
        .when(source_type.isNull() | ~source_type.isin(*expected_types), F.array(F.lit("unknown_source_type")))
        .when(version.isNull() | ~version.isin(*known_versions), F.array(F.lit("unknown_schema_version")))
        .otherwise(_non_null(checks))
    )
    ingest = F.col("ingest_time")
    late = ingest.isNotNull() & ts.isNotNull() & ((F.unix_timestamp(ingest) - F.unix_timestamp(ts)) > wm_seconds)
    flags.append(_reason(late, "late_arrival"))

    valid_coords = lat.between(-90.0, 90.0) & lon.between(-180.0, 180.0)
    out = df.select(
        F.lit(domain).alias("domain"),
        F.trim(event_id_raw).alias("event_id"),
        source_type.alias("source_type"),
        source_id_raw.alias("source_id"),
        version.alias("schema_version"),
        ts.alias("event_time"),
        F.to_date(ts).alias("event_date"),
        F.when(valid_coords, lat).alias("lat"),
        F.when(valid_coords, lon).alias("lon"),
        zone_id_col(F.when(valid_coords, lat), F.when(valid_coords, lon), grid).alias("zone_id"),
        (F.lower(j["synthetic"]) == "true").alias("is_synthetic"),
        # Provenance: real records state their source; synthetic ones are tagged as simulator output.
        F.coalesce(j["data_source"], F.when(F.lower(j["synthetic"]) == "true", F.lit("aegis-simulator")),
                   F.lit("unknown")).alias("data_source"),
        F.coalesce(j["observation_type"], F.when(F.lower(j["synthetic"]) == "true", F.lit("synthetic")),
                   F.lit("unknown")).alias("observation_type"),
        _non_null(flags).alias("dq_flags"),
        reasons.alias("reasons"),
        (F.size(reasons) == 0).alias("is_valid"),
        "topic", "origin", "kafka_partition", "kafka_offset", "source_seq", "ingest_time",
        F.current_timestamp().alias("processing_time"),
        F.col("value").alias("raw_value"),
        *[c.alias(name) for name, c in extra.items()],
    )
    return out


def canonicalize_all(df_raw: DataFrame, cfg: dict) -> dict[str, DataFrame]:
    """Split raw records by topic and canonicalise each domain."""
    mapping = topic_domains(cfg)
    return {domain: canonicalize(df_raw.filter(F.col("topic") == topic), domain, cfg)
            for topic, domain in mapping.items()}


def union_canonical(frames: Iterable[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), list(frames))


DOMAIN_COLUMN_TYPES = {
    "temperature_c": "double", "humidity_pct": "double", "rainfall_mm_hr": "double", "condition": "string",
    "condition_raw": "string",
    "river_id": "string", "station_name": "string", "water_level_m": "double", "danger_level_m": "double",
    "level_ratio": "double", "sensor_status": "string", "unit_raw": "string",
    "segment_id": "string", "density_veh_per_km": "double", "avg_speed_kmph": "double",
    "free_flow_speed_kmph": "double", "speed_ratio": "double", "disruption_level": "string", "is_disrupted": "boolean",
    "incident_id": "string", "category": "string", "category_raw": "string", "severity": "string",
    "description": "string", "channel": "string", "verification_status": "string", "confidence": "double",
    "report_weight": "double", "is_flood_related": "boolean", "affected_road": "string",
    "affected_area_km2": "double", "has_location": "boolean",
}


def with_all_domain_columns(df: DataFrame) -> DataFrame:
    """Add every domain column the frame lacks as a typed NULL (= not observed).

    Needed when silver tables are read back and some domain has no data yet
    (e.g. no live incident reports so far), so downstream integration code can
    reference all canonical columns."""
    for col, typ in DOMAIN_COLUMN_TYPES.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(typ))
    return df


def domain_view(df: DataFrame, domain: str) -> DataFrame:
    """Silver projection of one domain (canonical columns only)."""
    return df.filter(F.col("domain") == domain).select(*SILVER_COMMON, *DOMAIN_COLUMNS[domain])


def unknown_topic_records(df_raw: DataFrame, cfg: dict) -> DataFrame:
    return df_raw.filter(~F.col("topic").isin(*topic_domains(cfg).keys()))


def dedup_batch(df_valid: DataFrame) -> DataFrame:
    """Keep the first arrival of each event_id (earliest ingest, then arrival order)."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_time").asc_nulls_last(),
                                               F.col("source_seq").asc_nulls_last())
    return df_valid.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def quarantine_view(df: DataFrame, max_chars: int) -> DataFrame:
    return df.filter(~F.col("is_valid")).select(
        "domain", "topic", "origin", "event_id", "source_type", "schema_version", "reasons",
        F.array_distinct(F.transform("reasons", lambda r: F.split(r, ":")[0])).alias("reason_codes"),
        F.substring("raw_value", 1, max_chars).alias("raw_value"),
        "kafka_partition", "kafka_offset", "ingest_time", "processing_time",
        F.to_date("processing_time").alias("quarantine_date"),
    )


def dq_counts(df: DataFrame) -> DataFrame:
    """Per-domain totals: records, valid, rejected."""
    return df.groupBy("domain").agg(
        F.count(F.lit(1)).alias("records"),
        F.sum(F.col("is_valid").cast("int")).alias("valid"),
        F.sum((~F.col("is_valid")).cast("int")).alias("rejected"),
    )


def reason_counts(df: DataFrame) -> DataFrame:
    return (df.filter(~F.col("is_valid"))
            .select("domain", F.explode(F.array_distinct(F.transform("reasons", lambda r: F.split(r, ":")[0])))
                    .alias("reason"))
            .groupBy("domain", "reason").count())


def reason_detail_counts(df: DataFrame) -> DataFrame:
    return (df.filter(~F.col("is_valid")).select("domain", F.explode("reasons").alias("reason_detail"))
            .groupBy("domain", "reason_detail").count())


def flag_counts(df: DataFrame) -> DataFrame:
    return (df.filter(F.col("is_valid")).select("domain", F.explode("dq_flags").alias("flag"))
            .groupBy("domain", "flag").count())


def _interval_seconds(text: str) -> int:
    n, unit = text.split()
    unit = unit.lower().rstrip("s")
    return int(n) * {"second": 1, "minute": 60, "hour": 3600, "day": 86400}[unit]


interval_seconds = _interval_seconds
