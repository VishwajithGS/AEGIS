"""Read-only access to the Parquet lake with a short TTL cache.

Spark writes the lake; the API only reads it with pyarrow, so dashboard
requests never start Spark jobs. Partitioned tables use Hive-style
directories (``event_date=...``); files and directories starting with ``_``
or ``.`` (Spark's ``_SUCCESS``, ``_temporary``, ``.crc``) are ignored.

Streaming tables grow by one small file per micro-batch, and on the Windows
bind mount every ``stat`` costs ~5 ms. pyarrow's directory discovery stats
each file, which made ``/api/overview`` take > 60 s after a few hours of
streaming (~10 000 files). The store therefore lists files with
``os.walk`` (no per-file stat), builds the dataset from that list with
pyarrow's own Hive partition inference, reads fragments in parallel, and
remembers per-file row counts (Spark part files are immutable and uniquely
named).
"""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from aegis.config import data_path

_IGNORED_PREFIXES = (".", "_")          # same rule as pyarrow dataset discovery (any path component)
_PARALLEL_FILES = 16


def list_parquet_files(base: Path) -> list[str]:
    """Sorted data files under ``base``; ignored prefixes are not descended into.
    Directories that vanish while listing (Spark's ``_temporary``) are skipped."""
    out: list[str] = []
    for root, dirs, files in os.walk(base):
        dirs[:] = sorted(d for d in dirs if not d.startswith(_IGNORED_PREFIXES))
        out.extend(os.path.join(root, f) for f in files
                   if f.endswith(".parquet") and not f.startswith(_IGNORED_PREFIXES))
    return sorted(out)


class ParquetStore:
    def __init__(self, cfg: dict, ttl_s: float = 15.0):
        self.cfg = cfg
        self.ttl = ttl_s
        self._cache: dict = {}
        self._file_rows: dict[str, int] = {}
        self._lock = threading.Lock()

    def path(self, *parts: str) -> Path:
        return data_path(self.cfg, "lake_dir", *parts)

    def exists(self, *parts: str) -> bool:
        p = self.path(*parts)
        return p.exists() and bool(list_parquet_files(p))

    def _dataset(self, *parts: str) -> Optional[ds.Dataset]:
        base = self.path(*parts)
        files = list_parquet_files(base) if base.exists() else []
        if not files:
            return None
        posix = [Path(f).as_posix() for f in files]          # pyarrow paths use "/" (also on Windows)
        factory = ds.FileSystemDatasetFactory(
            pafs.LocalFileSystem(), posix, ds.ParquetFileFormat(),
            ds.FileSystemFactoryOptions(partition_base_dir=base.as_posix(), partitioning=ds.HivePartitioning.discover()))
        return factory.finish()

    def read(self, *parts: str, columns: Optional[list] = None) -> Optional[pd.DataFrame]:
        key = (parts, tuple(columns) if columns else None)
        now = time.time()
        with self._lock:
            hit = self._cache.get(key)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        for attempt in (1, 2):
            dset = self._dataset(*parts)
            if dset is None:
                return None
            cols = [c for c in columns if c in dset.schema.names] if columns else None
            try:
                df = dset.to_table(columns=cols, fragment_readahead=_PARALLEL_FILES).to_pandas()
                break
            except FileNotFoundError:
                if attempt == 2:                 # a file was removed after listing (compaction / overwrite)
                    raise
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
        n = self._count_rows(self.path(*parts))
        with self._lock:
            self._cache[key] = (now, n)
        return n

    def _count_rows(self, base: Path) -> int:
        """Sum of Parquet footer row counts; each footer is read once (in parallel)."""
        if not base.exists():
            return 0
        files = list_parquet_files(base)
        with self._lock:
            new = [f for f in files if f not in self._file_rows]
        if new:
            def rows(f: str) -> tuple[str, Optional[int]]:
                try:
                    return f, pq.read_metadata(f).num_rows
                except FileNotFoundError:            # removed between listing and reading
                    return f, None
            with ThreadPoolExecutor(max_workers=_PARALLEL_FILES) as ex:
                found = list(ex.map(rows, new))
            with self._lock:
                self._file_rows.update({f: n for f, n in found if n is not None})
        listed, prefix = set(files), os.path.join(str(base), "")
        with self._lock:
            for f in [f for f in self._file_rows if f.startswith(prefix) and f not in listed]:
                del self._file_rows[f]                  # file compacted away or overwritten
            return sum(self._file_rows.get(f, 0) for f in files)


def read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
