"""Compact small Parquet files written by Structured Streaming.

Each micro-batch appends one file per partition, so a long demo run creates
many small files. This job rewrites closed partitions (dates before today) of
the streaming tables into a few larger files. Run it while the streaming job
is stopped, or on dates the job no longer writes to.

    python -m spark_jobs.compact_lake               # all streaming tables
    python -m spark_jobs.compact_lake --include-today
"""
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from aegis.config import get_config
from spark_jobs.common import build_spark, lake

TABLES = {
    ("silver_stream", "weather"): "event_date", ("silver_stream", "water_level"): "event_date",
    ("silver_stream", "traffic"): "event_date", ("silver_stream", "incidents"): "event_date",
    ("live", "zone_risk"): "window_date", ("quarantine", "stream"): "quarantine_date",
}


def main() -> None:
    ap = argparse.ArgumentParser(description="Compact streaming Parquet partitions")
    ap.add_argument("--include-today", action="store_true")
    ap.add_argument("--files-per-partition", type=int, default=1)
    args = ap.parse_args()
    cfg = get_config()
    spark = build_spark("compact", cfg)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report = {}
    for parts, col in TABLES.items():
        base = Path(lake(cfg, *parts))
        if not base.exists():
            continue
        for pdir in sorted(base.glob(f"{col}=*")):
            date = pdir.name.split("=", 1)[1]
            if date >= today and not args.include_today:
                continue
            files = list(pdir.glob("*.parquet"))
            if len(files) <= args.files_per_partition:
                continue
            df = spark.read.parquet(str(pdir))
            n = df.count()
            tmp = pdir.with_name(pdir.name + ".compacting")
            df.coalesce(args.files_per_partition).write.mode("overwrite").parquet(str(tmp))
            if spark.read.parquet(str(tmp)).count() != n:
                shutil.rmtree(tmp, ignore_errors=True)
                raise SystemExit(f"row count mismatch while compacting {pdir}; original left untouched")
            shutil.rmtree(pdir)
            tmp.rename(pdir)
            report[f"{'/'.join(parts)}/{pdir.name}"] = {"files_before": len(files), "rows": n}
    print(json.dumps({"compacted_partitions": report}, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
