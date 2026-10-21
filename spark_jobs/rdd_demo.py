"""Spark RDD transformations and actions on AEGIS raw data.

Works directly on the raw JSON-lines payloads (generated historical files or
the small sample) so the low-level RDD API does its own parsing, mapping and
aggregation. Every step is labelled as a lazy TRANSFORMATION or an ACTION
(which triggers a Spark job). ``collect()`` is used only on small, already
aggregated results.

    python -m spark_jobs.rdd_demo                      # data/generated/historical
    python -m spark_jobs.rdd_demo --input data/sample  # small sample
"""
from __future__ import annotations

import argparse
import json
import re
from operator import add
from pathlib import Path

from pyspark.sql import functions as F

from aegis import canonical as C
from aegis.config import data_path, get_config
from spark_jobs.common import Timer, build_spark, reports_dir, write_json

STOPWORDS = {"the", "a", "an", "on", "in", "of", "to", "and", "after", "due", "has", "is", "along", "into",
             "synthetic", "report", "record", "incident", "archived"}
_ops: list[dict] = []


def op(kind: str, name: str, purpose: str) -> None:
    _ops.append({"type": kind, "operation": name, "purpose": purpose})
    tag = "TRANSFORMATION (lazy)" if kind == "transformation" else "ACTION (runs a job)"
    print(f"  [{tag:22}] {name:16} {purpose}")


def parse_json(line: str):
    try:
        obj = json.loads(line)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        return None


def norm_category(raw) -> str:
    return C.INCIDENT_CATEGORIES.get(str(raw).strip().lower(), "other") if raw is not None else "missing"


def rainfall_mm_hr(e: dict):
    """Source-to-canonical mapping inside the RDD pipeline (two vendor formats)."""
    try:
        if e.get("schema_version") == "wx-imperial-2":
            return e.get("station"), float(e["precip_in_hr"]) * C.INCH_TO_MM
        return e.get("source_id"), float(e["rainfall_mm_hr"])
    except (KeyError, TypeError, ValueError):
        return None


def run(spark, cfg: dict, base: Path) -> dict:
    """Run the demonstration on ``base/{incidents,weather}.jsonl`` and return the results."""
    _ops.clear()
    timer = Timer()
    sc = spark.sparkContext
    results: dict = {"input": str(base)}

    print("\n=== 1. Incidents by category (textFile -> map -> filter -> map -> reduceByKey -> sortBy) ===")
    lines = sc.textFile(str(base / "incidents.jsonl"))
    op("transformation", "textFile", "read raw JSON lines (one partition per file split)")
    parsed = lines.map(parse_json)
    op("transformation", "map", "parse JSON; malformed lines become None")
    valid = parsed.filter(lambda e: e is not None and e.get("event_id") and e.get("category"))
    op("transformation", "filter", "drop malformed records and records missing id/category")
    pairs = valid.map(lambda e: (norm_category(e["category"]), 1))
    op("transformation", "map", "normalise free-text category -> (category, 1)")
    by_cat = pairs.reduceByKey(add)
    op("transformation", "reduceByKey", "sum counts per category (shuffle)")
    ranked = by_cat.sortBy(lambda kv: kv[1], ascending=False)
    op("transformation", "sortBy", "order categories by count, descending")
    total_lines = lines.count()
    op("action", "count", f"raw lines = {total_lines}")
    n_valid = valid.count()
    op("action", "count", f"valid records = {n_valid}")
    first = valid.first()
    op("action", "first", f"first valid record id = {first.get('event_id')}")
    cat_rows = ranked.collect()
    op("action", "collect", f"{len(cat_rows)} category rows (small, aggregated)")
    for c, n in cat_rows:
        print(f"      {c:18} {n}")
    results["incidents_by_category"] = dict(cat_rows)
    results["raw_lines"], results["valid_records"], results["malformed_or_incomplete"] = (
        total_lines, n_valid, total_lines - n_valid)

    print("\n=== 2. Distinct sources and keywords (distinct, flatMap, take) ===")
    channels = valid.map(lambda e: (e.get("source_type"), e.get("channel") or "archive")).distinct()
    op("transformation", "distinct", "unique (source_type, channel) pairs")
    channel_rows = sorted(channels.collect(), key=str)
    op("action", "collect", f"{len(channel_rows)} distinct source/channel pairs")
    print("     ", channel_rows)
    words = (valid.flatMap(lambda e: re.findall(r"[a-z]+", str(e.get("description", "")).lower()))
             .filter(lambda w: len(w) > 3 and w not in STOPWORDS))
    op("transformation", "flatMap", "split descriptions into words (one record -> many)")
    top_words = words.map(lambda w: (w, 1)).reduceByKey(add).sortBy(lambda kv: -kv[1]).take(8)
    op("action", "take", "top 8 description keywords")
    print("     ", top_words)
    results["distinct_source_channels"] = [list(x) for x in channel_rows]
    results["top_keywords"] = top_words

    print("\n=== 3. Rainfall statistics per station (mapValues, aggregateByKey, reduce) ===")
    wx = sc.textFile(str(base / "weather.jsonl")).map(parse_json).filter(lambda e: e is not None)
    rain = wx.map(rainfall_mm_hr).filter(lambda kv: kv is not None and kv[0] and 0.0 <= kv[1] <= 300.0)
    op("transformation", "map+filter", "map both vendor formats to (station, rainfall mm/hr), drop invalid")
    stats = rain.aggregateByKey((0, 0.0, 0.0),
                                lambda acc, v: (acc[0] + 1, acc[1] + v, max(acc[2], v)),
                                lambda a, b: (a[0] + b[0], a[1] + b[1], max(a[2], b[2])))
    op("transformation", "aggregateByKey", "(count, sum, max) per station")
    per_station = stats.mapValues(lambda a: {"obs": a[0], "mean_mm_hr": round(a[1] / a[0], 3), "max_mm_hr": round(a[2], 2)})
    op("transformation", "mapValues", "derive mean intensity per station")
    station_rows = per_station.sortBy(lambda kv: -kv[1]["mean_mm_hr"]).collect()
    op("action", "collect", f"{len(station_rows)} station summaries")
    for sid, s in station_rows[:6]:
        print(f"      {sid:8} {s}")
    peak = rain.map(lambda kv: kv[1]).reduce(max)
    op("action", "reduce", f"peak rainfall intensity across all stations = {peak:.2f} mm/hr")
    wet_obs = rain.filter(lambda kv: kv[1] >= 2.5).count()
    op("action", "count", f"observations with >= 2.5 mm/hr = {wet_obs}")
    results["rainfall_per_station"] = dict(station_rows)
    results["peak_rainfall_mm_hr"] = round(peak, 3)
    results["wet_observations"] = wet_obs

    print("\n=== 4. Lineage of the category pipeline (toDebugString) ===")
    lineage = ranked.toDebugString()
    print(lineage.decode("utf-8") if isinstance(lineage, bytes) else lineage)

    print("\n=== 5. Verification: RDD result vs DataFrame API on the same input ===")
    df = spark.read.text(str(base / "incidents.jsonl"))
    j = F.from_json("value", "event_id string, category string")
    df_counts = (df.select(j.alias("j")).filter(F.col("j.event_id").isNotNull() & F.col("j.category").isNotNull())
                 .groupBy(F.lower(F.trim("j.category")).alias("raw_cat")).count().collect())
    df_total = {}
    for r in df_counts:
        k = C.INCIDENT_CATEGORIES.get(r["raw_cat"], "other")
        df_total[k] = df_total.get(k, 0) + r["count"]
    match = df_total == results["incidents_by_category"]
    print(f"      RDD and DataFrame category counts identical: {match}")
    results["verification_rdd_equals_dataframe"] = match
    results["operations"] = _ops
    results["duration_s"] = timer.seconds()
    write_json(reports_dir(cfg) / "rdd_demo.json", results)
    print(f"\nRDD demo finished in {results['duration_s']} s; results -> {reports_dir(cfg) / 'rdd_demo.json'}")
    return results


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="AEGIS Spark RDD demonstration")
    ap.add_argument("--input", default=str(data_path(cfg, "generated_dir", "historical")))
    args = ap.parse_args()
    base = Path(args.input)
    if not (base / "incidents.jsonl").exists():
        raise SystemExit(f"{base}/incidents.jsonl not found; run the data generator first")
    spark = build_spark("rdd-demo", cfg)
    try:
        run(spark, cfg, base)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
