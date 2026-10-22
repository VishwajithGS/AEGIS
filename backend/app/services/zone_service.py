"""Zone geometry and current rule-based risk.

Source order for the *current* assessment of each zone:

1. HBase ``zone_risk_history`` (latest row per zone by reverse prefix scan) -
   written by the streaming job (``data_origin = live-stream``) and optionally
   by the batch ETL (``batch-historical``);
2. ``data/state/zone_risk_latest.json`` - snapshot kept by the streaming job
   (used when HBase is unreachable);
3. ``gold/zone_risk_hourly`` - latest historical hour from the batch ETL.

Every zone carries its ``data_origin`` and a ``stale`` flag so the dashboard
never presents old or historical numbers as live.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from aegis.config import data_path
from aegis.geo import ZoneGrid, nearest_station_map
from aegis.storage.hbase_repository import HBaseUnavailable
from backend.app.services.hbase_service import HBaseService
from backend.app.services.parquet_store import ParquetStore, read_json

RISK_FIELDS = ["risk_score", "risk_category", "confidence", "coverage", "explanation", "contributions",
               "available_features", "missing_features", "rainfall_avg_mm_hr", "rainfall_max_mm_hr",
               "level_ratio_max", "weighted_reports", "reports", "disrupted_ratio", "speed_ratio_avg",
               "historical_hazard_index", "weather_obs", "water_obs", "traffic_obs", "window_start",
               "window_end", "assessed_at", "data_origin"]


def _iso(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and v != v):
        return None
    if isinstance(v, pd.Timestamp):
        return (v.tz_convert("UTC") if v.tzinfo else v.tz_localize("UTC")).isoformat()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _parse_ts(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        t = pd.Timestamp(v)
        return (t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")).to_pydatetime()
    except (ValueError, TypeError):
        return None


def _jsonish(v):
    if isinstance(v, str) and v[:1] in "[{":
        try:
            return json.loads(v)
        except ValueError:
            return v
    if hasattr(v, "tolist"):
        return v.tolist()
    return v


class ZoneService:
    def __init__(self, cfg: dict, store: ParquetStore, hbase: HBaseService, stale_after_s: float, ttl_s: float = 5.0):
        self.cfg, self.store, self.hbase = cfg, store, hbase
        self.grid = ZoneGrid.from_config(cfg)
        self.stale_after_s = stale_after_s
        self.ttl = ttl_s
        self._cache: tuple[float, Optional[dict]] = (0.0, None)
        self._lock = threading.Lock()
        self._stations = None

    def geometry(self) -> list[dict]:
        return [{"zone_id": z.zone_id, "row": z.row, "col": z.col,
                 "bounds": [[z.min_lat, z.min_lon], [z.max_lat, z.max_lon]],
                 "center": [round(z.center[0], 6), round(z.center[1], 6)]} for z in self.grid.zones()]

    def stations(self) -> dict:
        if self._stations is None:
            reg = read_json(data_path(self.cfg, "reference_dir", "stations.json")) or {}
            zones = self.grid.zones()
            r = self.cfg["region"]
            self._stations = {
                "registry": reg,
                "weather": nearest_station_map(zones, reg.get("weather_stations", []), r["weather_station_radius_km"]),
                "water": nearest_station_map(zones, reg.get("water_stations", []), r["water_station_radius_km"]),
            }
        return self._stations

    # -- sources -------------------------------------------------------------
    def _from_hbase(self) -> tuple[dict, Optional[str]]:
        if not self.hbase.enabled:          # deliberately off (e.g. real-data mode): not an error
            return {}, None
        try:
            rows = self.hbase.latest_zone_risk([z.zone_id for z in self.grid.zones()])
            return {z: {k: _jsonish(v) for k, v in r.items()} for z, r in rows.items()}, None
        except HBaseUnavailable as exc:
            return {}, str(exc)

    def _from_snapshot(self) -> dict:
        doc = read_json(data_path(self.cfg, "state_dir", "zone_risk_latest.json")) or {}
        return {z: {k: _jsonish(v) for k, v in r.items()} for z, r in (doc.get("zones") or {}).items()}

    def _from_batch(self) -> dict:
        df = self.store.read("gold", "zone_risk_hourly")
        if df is None or df.empty:
            return {}
        latest = df["hour"].max()
        out = {}
        for r in df[df["hour"] == latest].to_dict("records"):
            r = {k: _jsonish(v) for k, v in r.items()}
            r["window_start"] = _iso(r.pop("hour"))
            r["window_end"] = _iso(pd.Timestamp(r["window_start"]) + pd.Timedelta(hours=1))
            r["data_origin"] = "batch-historical"
            out[r["zone_id"]] = r
        return out

    # -- public --------------------------------------------------------------
    def current(self) -> dict:
        with self._lock:
            ts, cached = self._cache
            if cached is not None and time.time() - ts < self.ttl:
                return cached
        sources, errors = {}, {}
        hb, err = self._from_hbase()
        if err:
            errors["hbase"] = err
        snap = self._from_snapshot()
        batch = self._from_batch()
        now = datetime.now(timezone.utc)
        zones = []
        for g in self.geometry():
            z = g["zone_id"]
            cands = [(name, r) for name, r in (("hbase", hb.get(z)), ("snapshot", snap.get(z))) if r]
            if cands:
                origin_store, best = max(cands, key=lambda nr: str(nr[1].get("window_start")
                                                                    or nr[1].get("window_start_iso") or ""))
            elif z in batch:
                origin_store, best = "batch-gold", batch[z]
            else:
                origin_store, best = None, None
            rec = dict(g)
            if best:
                a = {k: _jsonish(best.get(k)) for k in RISK_FIELDS}
                for k in ("window_start", "window_end", "assessed_at"):
                    a[k] = _iso(_parse_ts(best.get(k) or best.get(f"{k}_iso"))) or a.get(k)
                end = _parse_ts(a.get("window_end"))
                a["stale"] = bool(a.get("data_origin") != "live-stream" or end is None
                                  or (now - end).total_seconds() > self.stale_after_s)
                a["read_from"] = origin_store
                rec["assessment"] = a
            else:
                rec["assessment"] = None
            zones.append(rec)
            sources[z] = origin_store
        cats: dict = {}
        for z in zones:
            c = (z["assessment"] or {}).get("risk_category") or "No assessment"
            cats[c] = cats.get(c, 0) + 1
        live = [z for z in zones if z["assessment"] and z["assessment"].get("data_origin") == "live-stream"]
        result = {
            "generated_at": now.isoformat(), "zones": zones, "category_counts": cats,
            "live_zones": len(live), "fresh_live_zones": sum(1 for z in live if not z["assessment"]["stale"]),
            "latest_window_end": max((z["assessment"]["window_end"] for z in zones
                                      if z["assessment"] and z["assessment"].get("window_end")), default=None),
            "source_errors": errors,
            "note": "Rule-based demonstration index (see docs). Zones are an analysis grid, not official boundaries.",
        }
        with self._lock:
            self._cache = (time.time(), result)
        return result

    def detail(self, zone_id: str) -> Optional[dict]:
        if self.grid.get(zone_id) is None:
            return None
        cur = next(z for z in self.current()["zones"] if z["zone_id"] == zone_id)
        history, err = [], None
        try:
            for r in self.hbase.zone_history(zone_id, 48):
                history.append({k: _jsonish(r.get(k)) for k in ("risk_score", "risk_category", "confidence",
                                                                 "data_origin", "rainfall_avg_mm_hr",
                                                                 "level_ratio_max", "weighted_reports")}
                               | {"window_start": r.get("window_start_iso"), "row_key": r.get("row_key")})
        except HBaseUnavailable as exc:
            err = str(exc)
        versions = []
        if history and not err:
            try:
                versions = self.hbase.score_versions(history[0]["row_key"])
            except HBaseUnavailable:
                pass
        batch_hist = []
        df = self.store.read("gold", "zone_risk_hourly", columns=["zone_id", "hour", "risk_score", "risk_category",
                                                                  "rainfall_avg_mm_hr", "level_ratio_max"])
        if df is not None and not df.empty:
            d = df[df["zone_id"] == zone_id].sort_values("hour").tail(72)
            batch_hist = [{"hour": _iso(r["hour"]), "risk_score": _jsonish(r["risk_score"]),
                           "risk_category": r["risk_category"], "rainfall_avg_mm_hr": _jsonish(r["rainfall_avg_mm_hr"]),
                           "level_ratio_max": _jsonish(r["level_ratio_max"])} for r in d.to_dict("records")]
        st = self.stations()
        ml = read_json(data_path(self.cfg, "state_dir", "ml_zone_predictions_latest.json")) or {}
        ml_zone = next((p for p in ml.get("predictions", []) if p.get("zone_id") == zone_id), None)
        return {
            **cur,
            "weather_station": st["weather"].get(zone_id), "water_gauge": st["water"].get(zone_id),
            "live_history_hbase": history, "hbase_error": err, "latest_row_score_versions": versions,
            "batch_history_72h": batch_hist,
            "ml_prediction": ({**ml_zone, "model_version": ml.get("model_version"), "features_as_of": ml.get("features_as_of"),
                               "kind": "ML-predicted flood-incident severity (synthetic-label model)"} if ml_zone else None),
        }
