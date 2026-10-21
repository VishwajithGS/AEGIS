"""Read-only access to the Parquet lake with a short TTL cache.

Spark writes the lake; the API only reads it with pyarrow, so dashboard
requests never start Spark jobs. Partitioned tables use Hive-style
directories (``event_date=...``); files starting with ``_`` or ``.``
(Spark's ``_SUCCESS``, ``_temporary``, ``.crc``) are ignored.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.dataset as ds

from aegis.config import data_path


class ParquetStore:
    def __init__(self, cfg: dict, ttl_s: float = 15.0):
        self.cfg = cfg
        self.ttl = ttl_s
        self._cache: dict = {}
        self._lock = threading.Lock()

    def path(self, *parts: str) -> Path:
        return data_path(self.cfg, "lake_dir", *parts)

    def exists(self, *parts: str) -> bool:
        p = self.path(*parts)
        return p.exists() and any(p.rglob("*.parquet"))

    def _dataset(self, *parts: str) -> Optional[ds.Dataset]:
        if not self.exists(*parts):
            return None
        return ds.dataset(str(self.path(*parts)), format="parquet", partitioning="hive")

    def read(self, *parts: str, columns: Optional[list] = None) -> Optional[pd.DataFrame]:
        key = (parts, tuple(columns) if columns else None)
        now = time.time()
        with self._lock:
            hit = self._cache.get(key)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        dset = self._dataset(*parts)
        if dset is None:
            return None
        cols = [c for c in columns if c in dset.schema.names] if columns else None
        df = dset.to_table(columns=cols).to_pandas()
        with self._lock:
            self._cache[key] = (now, df)
        return df

    def count(self, *parts: str) -> int:
        key = ("count",) + parts
        now = time.time()
        with self._lock:
            hit = self._cache.get(key)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        dset = self._dataset(*parts)
        n = int(dset.count_rows()) if dset is not None else 0
        with self._lock:
            self._cache[key] = (now, n)
        return n


def read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
