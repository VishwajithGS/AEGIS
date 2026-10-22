"""ParquetStore.count: same result as pyarrow dataset discovery, but footers are
read once per file, so growing streaming tables stay cheap to count."""
from __future__ import annotations

import shutil

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from backend.app.services.parquet_store import ParquetStore


def _part(path, n):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"x": list(range(n))}), path)


def test_count_matches_pyarrow_and_follows_file_changes(tmp_path):
    cfg = {"storage": {"data_root": str(tmp_path), "lake_dir": "lake"}}
    store = ParquetStore(cfg, ttl_s=0)
    base = store.path("silver_stream", "weather")
    _part(base / "event_date=2026-10-21" / "part-0.parquet", 3)
    _part(base / "event_date=2026-10-22" / "part-1.parquet", 4)
    _part(base / "_temporary" / "part-9.parquet", 100)                 # ignored, like pyarrow does
    _part(base / "event_date=2026-10-22" / ".part-2.parquet", 100)     # ignored
    expected = ds.dataset(str(base), format="parquet", partitioning="hive").count_rows()
    assert store.count("silver_stream", "weather") == expected == 7

    _part(base / "event_date=2026-10-22" / "part-3.parquet", 5)        # new micro-batch file
    assert store.count("silver_stream", "weather") == 12
    shutil.rmtree(base / "event_date=2026-10-21")                       # partition compacted away
    assert store.count("silver_stream", "weather") == 9
    assert not any("2026-10-21" in f for f in store._file_rows)        # cache pruned
    assert store.count("silver_stream", "missing") == 0


def test_read_matches_pyarrow_directory_discovery(tmp_path):
    cfg = {"storage": {"data_root": str(tmp_path), "lake_dir": "lake"}}
    store = ParquetStore(cfg, ttl_s=0)
    base = store.path("bronze", "kafka_raw")
    for topic, day, n in (("weather-events", "2026-10-21", 2), ("traffic-events", "2026-10-22", 3)):
        _part(base / f"topic={topic}" / f"ingest_date={day}" / f"part-{topic}.parquet", n)
    _part(base / "_temporary" / "0" / "part-x.parquet", 50)
    ref = ds.dataset(str(base), format="parquet", partitioning="hive").to_table().to_pandas()
    got = store.read("bronze", "kafka_raw")
    key = ["topic", "x"]
    assert list(got.columns) == list(ref.columns) == ["x", "topic", "ingest_date"]
    assert got.sort_values(key).reset_index(drop=True).equals(ref.sort_values(key).reset_index(drop=True))
    assert store.read("bronze", "kafka_raw", columns=["topic", "nope"]).columns.tolist() == ["topic"]
    assert store.exists("bronze", "kafka_raw") and store.read("bronze", "missing") is None
    _part(store.path("only_tmp", "t") / "_temporary" / "part-0.parquet", 1)
    assert not store.exists("only_tmp", "t")                            # nothing but Spark temp files
