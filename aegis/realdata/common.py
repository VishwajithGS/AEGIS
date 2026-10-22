"""Shared pieces of the real-data ingestion layer.

* ``Provenance`` - who produced a record, how it was obtained, under which terms.
* ``fetch`` - HTTP download with retries and an on-disk cache keyed by the
  request, so re-running an import reuses the same raw bytes (idempotent) and
  every raw response is recorded with its SHA-256 in the manifest.
* ``stable_id`` - deterministic event ids (source + location + time), so the
  same observation always gets the same id and the ETL de-duplicates re-imports.

Nothing here ever substitutes synthetic values: a failed download raises
``RealDataError`` and the import stops.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

USER_AGENT = "AEGIS-academic-project/1.0 (real-data ingestion; contact via repository)"


class RealDataError(RuntimeError):
    """A real data source could not be obtained or did not match its expected schema."""


@dataclass(frozen=True)
class Provenance:
    data_source: str          # short id, e.g. "era5-open-meteo"
    observation_type: str     # observation | observation_gridded | reanalysis | model | official_event | official_warning
    source_url: str           # dataset landing page / API base
    licence: str              # licence or terms as verified (see docs/dataset_migration_plan.md)
    citation: str = ""

    def as_fields(self) -> dict:
        return {"data_source": self.data_source, "observation_type": self.observation_type,
                "source_url": self.source_url, "licence": self.licence}


OBSERVATION_TYPES = {"observation", "observation_gridded", "reanalysis", "model", "official_event",
                     "official_warning", "synthetic"}


def utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("naive datetime; real-data timestamps must carry a time zone")
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def stable_id(prefix: str, *parts) -> str:
    h = hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{h}"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _cache_key(method: str, url: str, body: Optional[bytes]) -> str:
    return hashlib.sha256((method + " " + url + " " + (body or b"").decode("latin-1")).encode("utf-8")).hexdigest()[:24]


@dataclass
class FetchResult:
    url: str
    method: str
    status: int
    sha256: str
    size: int
    cached: bool
    retrieved_at: str
    path: str

    def manifest(self) -> dict:
        return asdict(self)


def fetch(url: str, cache_dir: Path, *, params: Optional[dict] = None, data: Optional[dict] = None,
          timeout: float = 120.0, retries: int = 4, backoff_s: float = 5.0, refresh: bool = False,
          suffix: str = ".bin") -> tuple[bytes, FetchResult]:
    """GET (or POST when ``data`` is given) with retries and a raw-response cache."""
    if params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params, safe=",:")
    body = urllib.parse.urlencode(data).encode("ascii") if data is not None else None
    method = "POST" if body is not None else "GET"
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _cache_key(method, url, body)
    raw_path = cache_dir / f"{key}{suffix}"
    meta_path = cache_dir / f"{key}.meta.json"      # distinct from the raw file even when suffix is .json
    if raw_path.exists() and meta_path.exists() and not refresh:
        raw = raw_path.read_bytes()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("sha256") == sha256_bytes(raw):
            return raw, FetchResult(**{**meta, "cached": True})
    last: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=body, method=method, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                status = resp.status
            if status != 200:
                raise RealDataError(f"HTTP {status} from {url}")
            res = FetchResult(url=url, method=method, status=status, sha256=sha256_bytes(raw), size=len(raw),
                              cached=False, retrieved_at=utc_iso(datetime.now(timezone.utc)), path=str(raw_path))
            tmp = raw_path.with_suffix(raw_path.suffix + ".tmp")
            tmp.write_bytes(raw)
            os.replace(tmp, raw_path)
            meta_path.write_text(json.dumps(res.manifest(), indent=2), encoding="utf-8")
            return raw, res
        except (urllib.error.URLError, TimeoutError, ConnectionError, RealDataError, OSError) as exc:
            last = exc
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (400, 401, 403, 404):
                break                       # not transient: report immediately
            if attempt < retries:
                time.sleep(backoff_s * attempt)
    raise RealDataError(f"could not download {url}: {last}") from last


def write_jsonl(path: Path, records: Iterable[dict]) -> int:
    """Atomic JSON-lines write (sorted keys -> byte-identical output for identical input)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    n = 0
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        for r in records:
            fh.write(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n")
            n += 1
    os.replace(tmp, path)
    return n


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    os.replace(tmp, path)


def require_fields(obj: dict, fields: Iterable[str], what: str) -> None:
    missing = [f for f in fields if f not in obj]
    if missing:
        raise RealDataError(f"{what}: response is missing expected fields {missing}")
