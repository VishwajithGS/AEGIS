"""Pipeline monitoring: service reachability, Kafka offsets, streaming query
progress, producer activity and staleness."""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

from aegis.config import data_path
from backend.app.services.hbase_service import HBaseService
from backend.app.services.parquet_store import ParquetStore, read_json


def _age_s(iso_or_epoch) -> Optional[float]:
    if iso_or_epoch is None:
        return None
    try:
        if isinstance(iso_or_epoch, (int, float)):
            t = float(iso_or_epoch)
        else:
            t = datetime.fromisoformat(str(iso_or_epoch).replace("Z", "+00:00")).timestamp()
        return round(time.time() - t, 1)
    except ValueError:
        return None


class PipelineService:
    def __init__(self, cfg: dict, store: ParquetStore, hbase: HBaseService, live_feed, model_service,
                 stale_after_s: float):
        self.cfg, self.store, self.hbase, self.feed, self.model = cfg, store, hbase, live_feed, model_service
        self.stale_after_s = stale_after_s
        self._kafka_cache: tuple[float, Optional[dict]] = (0.0, None)
        self._lock = threading.Lock()

    def kafka(self) -> dict:
        with self._lock:
            ts, cached = self._kafka_cache
            if cached is not None and time.time() - ts < 10:
                return cached
        try:
            from producers.kafka_admin import describe_topics
            topics = describe_topics(self.cfg, timeout=4)
            res = {"available": True, "bootstrap": self.cfg["kafka"]["bootstrap_servers"], "topics": topics}
        except Exception as exc:  # noqa: BLE001 - any client error means unreachable for the dashboard
            res = {"available": False, "bootstrap": self.cfg["kafka"]["bootstrap_servers"],
                   "error": f"{type(exc).__name__}: {exc}"}
        with self._lock:
            self._kafka_cache = (time.time(), res)
        return res

    def streaming(self) -> dict:
        st = read_json(data_path(self.cfg, "state_dir", "streaming", "status.json"))
        if not st:
            return {"available": False, "reason": "streaming job has not run yet"}
        queries = {}
        for name, q in (st.get("queries") or {}).items():
            lp = q.get("last_progress") or {}
            queries[name] = {"status": q.get("status"), "restarts": q.get("restarts", 0), "error": q.get("error"),
                             "batch_id": lp.get("batch_id"), "num_input_rows": lp.get("num_input_rows"),
                             "rows_scanned": lp.get("rows_scanned"),
                             "input_rows_per_second": lp.get("input_rows_per_second"),
                             "processed_rows_per_second": lp.get("processed_rows_per_second"),
                             "trigger_ms": lp.get("trigger_ms"), "watermark": lp.get("watermark"),
                             "state_rows": lp.get("state_rows"), "dropped_by_watermark": lp.get("dropped_by_watermark"),
                             "last_progress_at": lp.get("timestamp"),
                             "last_progress_age_s": _age_s(lp.get("timestamp"))}
        age = _age_s(st.get("updated_at"))
        running = st.get("state") == "running" and age is not None and age < 120
        return {"available": True, "state": st.get("state"), "running": running, "mode": st.get("mode"),
                "started_at": st.get("started_at"), "updated_at": st.get("updated_at"), "status_age_s": age,
                "hbase_sink": st.get("hbase_sink"), "watermark": st.get("watermark"), "window": st.get("window"),
                "queries": queries, "last_dq": st.get("last_dq"), "last_silver": st.get("last_silver")}

    def producers(self) -> list[dict]:
        out = []
        for p in sorted(data_path(self.cfg, "state_dir").glob("producer_*_status.json")):
            d = read_json(p) or {}
            age = _age_s(d.get("updated_at"))
            out.append({"name": p.stem.replace("producer_", "").replace("_status", ""),
                        "running": bool(d.get("running")) and age is not None and age < 60, "updated_age_s": age,
                        "rate_per_sec": d.get("rate_per_sec"), "producer": d.get("producer") or
                        {k: d.get(k) for k in ("sent", "delivered", "failed")}, "generator": d.get("generator") or
                        d.get("generator_counts")})
        return out

    def status(self) -> dict:
        hb = self.hbase.health()
        kafka = self.kafka()
        stream = self.streaming()
        model = self.model.status()
        feed = dict(self.feed.status)
        counts = {
            "bronze_kafka_raw": self.store.count("bronze", "kafka_raw"),
            "silver_stream": {d: self.store.count("silver_stream", d) for d in ("weather", "water_level", "traffic", "incidents")},
            "silver_batch": {d: self.store.count("silver", d) for d in ("weather", "water_level", "traffic", "incidents")},
            "quarantine_stream": self.store.count("quarantine", "stream"),
            "quarantine_batch": self.store.count("quarantine", "batch"),
            "live_zone_risk_rows": self.store.count("live", "zone_risk"),
        }
        metrics = {}
        if hb.get("available"):
            try:
                metrics = self.hbase.latest_metrics(["stream.bronze_dq", "stream.silver", "stream.zone_risk"])
            except Exception:  # noqa: BLE001
                metrics = {}
        issues = []
        if not kafka["available"]:
            issues.append("Kafka unreachable")
        if not hb.get("available"):
            issues.append("HBase unreachable")
        if not stream.get("running"):
            issues.append("streaming job not running (live data may be stale)")
        for n, q in (stream.get("queries") or {}).items():
            if q.get("status") == "failed":
                issues.append(f"streaming query {n} failed: {q.get('error')}")
        overall = "ok" if not issues else ("down" if not kafka["available"] and not hb.get("available") else "degraded")
        return {"generated_at": datetime.now(timezone.utc).isoformat(), "overall": overall, "issues": issues,
                "services": {"kafka": kafka, "hbase": hb, "streaming": stream, "live_feed": feed,
                             "model": {k: model.get(k) for k in ("state", "reason", "version")}},
                "producers": self.producers(), "lake_row_counts": counts, "hbase_processing_metrics": metrics,
                "batch_reports": {
                    "dq_report_generated_at": (read_json(data_path(self.cfg, "reports_dir", "dq_report_batch.json")) or {}).get("generated_at"),
                    "ml_trained_at": model.get("trained_at")}}
