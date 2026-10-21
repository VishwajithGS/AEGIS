"""SparkSession construction and lake path helpers shared by all jobs."""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from aegis.config import data_path, get_config

# Lake layout (relative to storage.lake_dir):
#   bronze/kafka_raw        raw Kafka records archived by the streaming job
#   bronze/files            raw generated files materialised by the batch ETL
#   quarantine/{stream,batch}  rejected records with reasons
#   silver/<domain>         batch ETL canonical tables (weather, water_level, traffic, incidents)
#   silver_stream/<domain>  canonical tables written by Structured Streaming
#   gold/<table>            analytics-ready batch outputs
#   live/<table>            streaming analytics outputs (zone risk, DQ metrics, ML predictions)
DOMAINS = ("weather", "water_level", "traffic", "incidents")


def build_spark(app_name: str, cfg: Optional[dict] = None, master: Optional[str] = None,
                shuffle_partitions: Optional[int] = None, driver_memory: Optional[str] = None,
                ui: bool = False, extra: Optional[dict] = None) -> SparkSession:
    cfg = cfg or get_config()
    s = cfg["spark"]
    ui = ui or os.environ.get("AEGIS_SPARK_UI") == "1"
    builder = (
        SparkSession.builder.appName(f"{s['app_name']}-{app_name}")
        .master(master or os.environ.get("AEGIS_SPARK_MASTER") or s["master"])
        .config("spark.sql.session.timeZone", s["session_timezone"])
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={s['session_timezone']}")
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={s['session_timezone']}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or s["shuffle_partitions"]))
        .config("spark.driver.memory", driver_memory or s["driver_memory"])
        .config("spark.sql.ansi.enabled", "false")              # invalid casts -> NULL (detected as invalid_type)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # unparseable timestamps -> NULL, not an exception
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")   # UTC-adjusted, readable by pyarrow
        .config("spark.ui.enabled", "true" if ui else "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.metricsEnabled", "true")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(s["log_level"])
    return spark


def lake(cfg: dict, *parts: str) -> str:
    return str(data_path(cfg, "lake_dir", *parts))


def checkpoint(cfg: dict, name: str) -> str:
    return str(data_path(cfg, "checkpoint_dir", name))


def reports_dir(cfg: dict) -> Path:
    p = data_path(cfg, "reports_dir")
    p.mkdir(parents=True, exist_ok=True)
    return p


def path_exists(path: str) -> bool:
    p = Path(path)
    return p.exists() and any(p.rglob("*.parquet"))


def read_parquet_if_exists(spark: SparkSession, path: str) -> Optional[DataFrame]:
    return spark.read.parquet(path) if path_exists(path) else None


def write_json(path: Path, obj) -> None:
    """Atomic JSON write (temp file + replace) so readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def remove_dir(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)


class Timer:
    def __init__(self):
        self.t0 = time.time()

    def seconds(self) -> float:
        return round(time.time() - self.t0, 3)
