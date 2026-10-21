"""Incident explorer and nearby-hazard search.

Incidents come from the Parquet silver tables: ``silver/incidents`` (batch:
archived incidents + historical reports) and ``silver_stream/incidents``
(live reports from Structured Streaming). Point lookups also try HBase
``incident_records``. Distances use the Haversine formula (km).
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from aegis.geo import haversine_km_vec
from aegis.storage.hbase_repository import HBaseUnavailable
from backend.app.services.hbase_service import HBaseService
from backend.app.services.parquet_store import ParquetStore

COLUMNS = ["incident_id", "event_id", "source_type", "source_id", "category", "category_raw", "severity",
           "description", "channel", "verification_status", "confidence", "is_flood_related", "lat", "lon",
           "zone_id", "event_time", "dq_flags", "affected_road", "is_synthetic"]


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.floating,)):
        return None if np.isnan(v) else float(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return (v.tz_convert("UTC") if v.tzinfo else v.tz_localize("UTC")).isoformat()
    if isinstance(v, np.ndarray):
        return v.tolist()
    return v


class IncidentService:
    def __init__(self, store: ParquetStore, hbase: HBaseService):
        self.store, self.hbase = store, hbase

    def frame(self) -> pd.DataFrame:
        parts = []
        for origin, path in (("batch-historical", ("silver", "incidents")), ("live-stream", ("silver_stream", "incidents"))):
            df = self.store.read(*path, columns=COLUMNS)
            if df is not None and not df.empty:
                parts.append(df.assign(data_origin=origin))
        if not parts:
            return pd.DataFrame(columns=COLUMNS + ["data_origin"])
        df = pd.concat(parts, ignore_index=True)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        return df.drop_duplicates("incident_id", keep="last").sort_values("event_time", ascending=False)

    @staticmethod
    def to_records(df: pd.DataFrame) -> list[dict]:
        return [{k: _clean(v) for k, v in r.items()} for r in df.to_dict("records")]

    def list(self, category: Optional[str] = None, source_type: Optional[str] = None,
             verification: Optional[str] = None, zone_id: Optional[str] = None, origin: Optional[str] = None,
             q: Optional[str] = None, since_hours: Optional[float] = None, located_only: bool = False,
             limit: int = 50, offset: int = 0) -> dict:
        df = self.frame()
        if category:
            df = df[df["category"] == category]
        if source_type:
            df = df[df["source_type"] == source_type]
        if verification:
            df = df[df["verification_status"] == verification]
        if zone_id:
            df = df[df["zone_id"] == zone_id]
        if origin:
            df = df[df["data_origin"] == origin]
        if located_only:
            df = df[df["lat"].notna() & df["lon"].notna()]
        if since_hours:
            ref = df["event_time"].max() if origin == "batch-historical" else pd.Timestamp.now(tz="UTC")
            df = df[df["event_time"] >= ref - pd.Timedelta(hours=since_hours)]
        if q:
            ql = q.lower()
            mask = (df["description"].fillna("").str.lower().str.contains(ql, regex=False)
                    | df["incident_id"].fillna("").str.lower().str.contains(ql, regex=False)
                    | df["category_raw"].fillna("").str.lower().str.contains(ql, regex=False))
            df = df[mask]
        total = int(len(df))
        page = df.iloc[offset: offset + limit]
        return {"total": total, "limit": limit, "offset": offset, "items": self.to_records(page)}

    def nearby(self, lat: float, lon: float, radius_km: float, limit: int = 100,
               origin: Optional[str] = None) -> dict:
        df = self.frame()
        df = df[df["lat"].notna() & df["lon"].notna() & (df["verification_status"] != "rejected")]
        if origin:
            df = df[df["data_origin"] == origin]
        if df.empty:
            return {"center": [lat, lon], "radius_km": radius_km, "total": 0, "items": [], "by_category": {}}
        d = haversine_km_vec(lat, lon, df["lat"].to_numpy(float), df["lon"].to_numpy(float))
        df = df.assign(distance_km=np.round(d, 3))
        df = df[df["distance_km"] <= radius_km].sort_values(["distance_km", "event_time"])
        return {"center": [lat, lon], "radius_km": radius_km, "unit": "km", "method": "haversine",
                "total": int(len(df)), "by_category": {k: int(v) for k, v in df["category"].value_counts().items()},
                "items": self.to_records(df.head(limit))}

    def get(self, incident_id: str) -> Optional[dict]:
        try:
            row = self.hbase.incident(incident_id)
            if row:
                return {"incident_id": incident_id, "read_from": "hbase:incident_records", "families": row}
        except HBaseUnavailable:
            pass
        df = self.frame()
        hit = df[df["incident_id"] == incident_id]
        if hit.empty:
            return None
        return {"read_from": "parquet", **self.to_records(hit.head(1))[0]}

    def summary(self) -> dict:
        df = self.frame()
        if df.empty:
            return {"total": 0}
        now = pd.Timestamp.now(tz="UTC")
        live = df[df["data_origin"] == "live-stream"]
        return {
            "total": int(len(df)),
            "archived_incidents": int((df["source_type"] == "historical_incident").sum()),
            "public_reports": int((df["source_type"] == "public_report").sum()),
            "live_reports_last_hour": int((live["event_time"] >= now - pd.Timedelta(hours=1)).sum()),
            "live_reports_last_24h": int((live["event_time"] >= now - pd.Timedelta(hours=24)).sum()),
            "by_category": {k: int(v) for k, v in df["category"].value_counts().items()},
            "latest_event_time": _clean(df["event_time"].max()),
        }
