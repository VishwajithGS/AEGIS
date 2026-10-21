"""HBase wide-column repository (via the HBase Thrift server and happybase).

Table design (see docs/architecture.md, "HBase schema"):

====================  =====================================  ==========================================
table                 row key                                column families (versions, TTL)
====================  =====================================  ==========================================
sensor_observations   source_id#event_ts_ms(13)#event_id     measurement(1, 30 d), location(1, 30 d),
                                                             metadata(1, 30 d)
incident_records      incident_id                            incident(1), location(1), verification(3)
zone_risk_history     zone_id#series#window_start_ms(13)     risk(5), summary(1), quality(1)
                      (series: live = streaming windows,
                       hourly = batch ETL)
processing_metrics    component#metric_ts_ms(13)             metrics(1, 7 d), status(1, 7 d)
model_predictions     subject#(MAX_TS - ts_ms)(13)#pred_id   input(1), output(1), meta(1)
====================  =====================================  ==========================================

Values are stored as UTF-8 strings; ``None`` values are not written at all,
so absent measurements cost nothing (sparse wide rows). Puts are idempotent:
re-processing the same event writes the same row key and cells.
"""
from __future__ import annotations

import json
import re
import socket
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional

DAY = 86400
MAX_TS_MS = 9_999_999_999_999  # 13 digits; reverse-timestamp base

TABLES: dict[str, dict] = {
    "sensor_observations": {
        "families": {
            "measurement": {"max_versions": 1, "time_to_live": 30 * DAY},
            "location": {"max_versions": 1, "time_to_live": 30 * DAY},
            "metadata": {"max_versions": 1, "time_to_live": 30 * DAY},
        },
        "row_key": "source_id#event_ts_ms#event_id",
    },
    "incident_records": {
        "families": {
            "incident": {"max_versions": 1},
            "location": {"max_versions": 1},
            "verification": {"max_versions": 3},
        },
        "row_key": "incident_id",
    },
    "zone_risk_history": {
        "families": {
            "risk": {"max_versions": 5},
            "summary": {"max_versions": 1},
            "quality": {"max_versions": 1},
        },
        "row_key": "zone_id#series#window_start_ms (series = live | hourly)",
    },
    "processing_metrics": {
        "families": {
            "metrics": {"max_versions": 1, "time_to_live": 7 * DAY},
            "status": {"max_versions": 1, "time_to_live": 7 * DAY},
        },
        "row_key": "component#metric_ts_ms",
    },
    "model_predictions": {
        "families": {
            "input": {"max_versions": 1},
            "output": {"max_versions": 1},
            "meta": {"max_versions": 1},
        },
        "row_key": "subject#reverse_ts_ms#prediction_id",
    },
}


class HBaseUnavailable(RuntimeError):
    """Raised when the HBase Thrift server cannot be reached."""


# ---------------------------------------------------------------------------
# Row keys
# ---------------------------------------------------------------------------
def _ts13(ms: int) -> str:
    if ms < 0 or ms > MAX_TS_MS:
        raise ValueError(f"timestamp out of range for a 13-digit key: {ms}")
    return f"{int(ms):013d}"


def sensor_row_key(source_id: str, event_ts_ms: int, event_id: str) -> str:
    return f"{source_id}#{_ts13(event_ts_ms)}#{event_id}"


ZONE_RISK_SERIES = ("live", "hourly")   # streaming 5-minute windows | batch hourly assessments


def zone_risk_row_key(zone_id: str, window_start_ms: int, series: str = "live") -> str:
    """``zone_id#series#window_start_ms``. Live and batch assessments use separate
    key ranges, so a batch hour and a live window starting at the same instant never
    overwrite each other's cells (they have different window lengths and origins)."""
    if series not in ZONE_RISK_SERIES:
        raise ValueError(f"unknown zone-risk series {series!r}; expected one of {ZONE_RISK_SERIES}")
    return f"{zone_id}#{series}#{_ts13(window_start_ms)}"


def metric_row_key(component: str, ts_ms: int) -> str:
    return f"{component}#{_ts13(ts_ms)}"


def prediction_row_key(subject: str, ts_ms: int, prediction_id: str) -> str:
    """Reverse timestamp so a forward prefix scan returns the newest first."""
    return f"{subject}#{_ts13(MAX_TS_MS - int(ts_ms))}#{prediction_id}"


# ---------------------------------------------------------------------------
# Cell (de)serialisation
# ---------------------------------------------------------------------------
def encode_value(value: Any) -> Optional[bytes]:
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    if isinstance(value, bool):
        return b"true" if value else b"false"
    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, default=str).encode("utf-8")
    if hasattr(value, "isoformat"):
        return value.isoformat().encode("utf-8")
    return str(value).encode("utf-8")


def to_cells(families: Mapping[str, Mapping[str, Any]]) -> dict[bytes, bytes]:
    """``{"family": {"qualifier": value}}`` -> happybase put mapping, skipping None."""
    cells: dict[bytes, bytes] = {}
    for fam, cols in families.items():
        for q, v in cols.items():
            b = encode_value(v)
            if b is not None:
                cells[f"{fam}:{q}".encode("utf-8")] = b
    return cells


_INT_RE = re.compile(r"^-?\d{1,18}$")
_FLOAT_RE = re.compile(r"^-?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$")


def _decode_scalar(raw: bytes) -> Any:
    s = raw.decode("utf-8", "replace")
    if s in ("true", "false"):
        return s == "true"
    if s and s[0] in "[{":
        try:
            return json.loads(s)
        except ValueError:
            return s
    if _INT_RE.match(s):
        return int(s)
    if _FLOAT_RE.match(s):
        return float(s)
    return s


def from_cells(cells: Mapping[bytes, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for col, val in cells.items():
        fam, q = col.decode("utf-8").split(":", 1)
        out.setdefault(fam, {})[q] = _decode_scalar(val[0] if isinstance(val, tuple) else val)
    return out


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------
class HBaseRepository:
    def __init__(self, host: str = "localhost", port: int = 9090, timeout_ms: int = 20000,
                 batch_size: int = 500, write_retries: int = 3, retry_backoff_s: float = 2.0):
        self.host, self.port = host, int(port)
        self.timeout_ms = int(timeout_ms)
        self.batch_size = int(batch_size)
        self.write_retries = int(write_retries)
        self.retry_backoff_s = float(retry_backoff_s)
        self._conn = None

    @classmethod
    def from_config(cls, cfg: dict) -> "HBaseRepository":
        h = cfg["hbase"]
        return cls(h["host"], h["port"], h["timeout_ms"], h["batch_size"], h["write_retries"], h["retry_backoff_s"])

    # -- connection ---------------------------------------------------------
    def connect(self):
        """Open (or reuse) the Thrift connection. Retries ``write_retries`` times with
        linear backoff, so a transient network/DNS hiccup does not fail a whole batch job;
        the API uses ``write_retries=1`` to fail fast."""
        if self._conn is None:
            import happybase  # imported lazily so modules load without the client
            attempts = max(1, self.write_retries)
            last: Optional[Exception] = None
            for attempt in range(1, attempts + 1):
                try:
                    self._conn = happybase.Connection(self.host, self.port, timeout=self.timeout_ms, autoconnect=True)
                    self._conn.tables()   # forces a round trip
                    return self._conn
                except Exception as exc:  # noqa: BLE001 - thrift raises several transport errors
                    last = exc
                    self._conn = None
                    if attempt < attempts:
                        time.sleep(self.retry_backoff_s * attempt)
            raise HBaseUnavailable(f"HBase Thrift server not reachable at {self.host}:{self.port} "
                                   f"after {attempts} attempt(s): {last}") from last
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def __enter__(self) -> "HBaseRepository":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @staticmethod
    def _transient_errors() -> tuple:
        errors: list = [OSError, socket.error, EOFError]
        try:
            from thriftpy2.transport import TTransportException
            errors.append(TTransportException)
        except ImportError:  # pragma: no cover - thriftpy2 ships with happybase
            pass
        return tuple(errors)

    def _retry(self, fn: Callable[[], Any]) -> Any:
        last: Optional[Exception] = None
        for attempt in range(1, self.write_retries + 1):
            try:
                return fn()
            except (HBaseUnavailable,) + self._transient_errors() as exc:
                last = exc
                self.close()
                if attempt < self.write_retries:
                    time.sleep(self.retry_backoff_s * attempt)
        raise HBaseUnavailable(f"HBase operation failed after {self.write_retries} attempts: {last}") from last

    def health(self) -> dict:
        t0 = time.time()
        try:
            tables = sorted(t.decode() for t in self.connect().tables())
            return {"available": True, "tables": tables, "latency_ms": round((time.time() - t0) * 1000, 1),
                    "endpoint": f"{self.host}:{self.port}"}
        except HBaseUnavailable as exc:
            return {"available": False, "error": str(exc), "endpoint": f"{self.host}:{self.port}"}

    # -- DDL ----------------------------------------------------------------
    def ensure_tables(self) -> dict[str, str]:
        conn = self.connect()
        existing = {t.decode() for t in conn.tables()}
        result = {}
        for name, spec in TABLES.items():
            if name in existing:
                result[name] = "exists"
                continue
            conn.create_table(name, {fam: dict(opts) for fam, opts in spec["families"].items()})
            result[name] = "created"
        return result

    def describe(self, table: str) -> dict:
        fams = self.connect().table(table).families()
        return {f.decode(): {k: (v.decode() if isinstance(v, bytes) else v) for k, v in d.items()} for f, d in fams.items()}

    # -- writes -------------------------------------------------------------
    def put(self, table: str, row_key: str, families: Mapping[str, Mapping[str, Any]]) -> None:
        cells = to_cells(families)
        if not cells:
            return
        self._retry(lambda: self.connect().table(table).put(row_key.encode("utf-8"), cells))

    def put_many(self, table: str, rows: Iterable[tuple[str, Mapping[str, Mapping[str, Any]]]]) -> int:
        """Batched puts; returns the number of rows sent. The whole batch is
        retried on transport errors (safe because puts are idempotent)."""
        rows = list(rows)
        if not rows:
            return 0

        def _send() -> int:
            n = 0
            with self.connect().table(table).batch(batch_size=self.batch_size) as b:
                for key, fams in rows:
                    cells = to_cells(fams)
                    if cells:
                        b.put(key.encode("utf-8"), cells)
                        n += 1
            return n
        return self._retry(_send)

    def delete(self, table: str, row_key: str) -> None:
        self._retry(lambda: self.connect().table(table).delete(row_key.encode("utf-8")))

    # -- reads --------------------------------------------------------------
    def get(self, table: str, row_key: str) -> Optional[dict]:
        def _get():
            raw = self.connect().table(table).row(row_key.encode("utf-8"))
            return from_cells(raw) if raw else None
        return self._retry(_get)

    def cell_versions(self, table: str, row_key: str, column: str, versions: int = 5) -> list[dict]:
        def _cells():
            cells = self.connect().table(table).cells(row_key.encode(), column.encode(), versions=versions,
                                                      include_timestamp=True)
            return [{"value": _decode_scalar(v), "hbase_ts_ms": ts} for v, ts in cells]
        return self._retry(_cells)

    def scan(self, table: str, row_prefix: Optional[str] = None, limit: Optional[int] = None,
             reverse: bool = False, row_start: Optional[str] = None, row_stop: Optional[str] = None,
             columns: Optional[list[str]] = None) -> list[tuple[str, dict]]:
        def _scan():
            kwargs: dict[str, Any] = {"limit": limit, "reverse": reverse}
            if row_prefix is not None:
                kwargs["row_prefix"] = row_prefix.encode("utf-8")
            else:
                if row_start is not None:
                    kwargs["row_start"] = row_start.encode("utf-8")
                if row_stop is not None:
                    kwargs["row_stop"] = row_stop.encode("utf-8")
            if columns:
                kwargs["columns"] = [c.encode("utf-8") for c in columns]
            return [(k.decode("utf-8"), from_cells(v)) for k, v in self.connect().table(table).scan(**kwargs)]
        return self._retry(_scan)

    def latest_with_prefix(self, table: str, prefix: str) -> Optional[tuple[str, dict]]:
        rows = self.scan(table, row_prefix=prefix, limit=1, reverse=True)
        return rows[0] if rows else None

    def count_rows(self, table: str, row_prefix: Optional[str] = None, max_rows: int = 1_000_000) -> int:
        """Key-only count (FirstKeyOnlyFilter) for verification; bounded by ``max_rows``."""
        def _count():
            kwargs: dict[str, Any] = {"filter": b"FirstKeyOnlyFilter()", "limit": max_rows}
            if row_prefix:
                kwargs["row_prefix"] = row_prefix.encode("utf-8")
            return sum(1 for _ in self.connect().table(table).scan(**kwargs))
        return self._retry(_count)


@contextmanager
def repository(cfg: dict) -> Iterator[HBaseRepository]:
    repo = HBaseRepository.from_config(cfg)
    try:
        yield repo
    finally:
        repo.close()
