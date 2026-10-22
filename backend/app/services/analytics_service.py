"""Historical analytics and data-quality summaries from the lake."""
from __future__ import annotations

import json
from typing import Optional

import pandas as pd

from aegis.config import data_path
from backend.app.services.incident_service import _clean
from backend.app.services.parquet_store import ParquetStore, read_json


def _records(df: pd.DataFrame) -> list[dict]:
    return [{k: _clean(v) for k, v in r.items()} for r in df.to_dict("records")]


class AnalyticsService:
    def __init__(self, cfg: dict, store: ParquetStore):
        self.cfg, self.store = cfg, store

    def historical(self, zone_id: Optional[str] = None, days: Optional[int] = None) -> dict:
        out: dict = {"data_origin": "batch-historical", "zone_id": zone_id}
        inc = self.store.read("gold", "incident_daily")
        zd = self.store.read("gold", "zone_daily")
        if inc is None or zd is None:
            return {**out, "available": False, "reason": "batch gold tables not found; run the batch ETL"}
        zd = zd.copy()
        zd["event_date"] = pd.to_datetime(zd["event_date"])
        if days:
            cutoff = zd["event_date"].max() - pd.Timedelta(days=days - 1)
            zd = zd[zd["event_date"] >= cutoff]
        if zone_id:
            zd = zd[zd["zone_id"] == zone_id]
        daily = (zd.groupby("event_date").agg(rain_mm=("rain_mm", "mean"), rain_peak_mm_hr=("rain_peak_mm_hr", "max"),
                                              level_ratio_peak=("level_ratio_peak", "max"),
                                              risk_avg=("risk_avg", "mean"), risk_peak=("risk_peak", "max"),
                                              reports=("reports", "sum")).reset_index().sort_values("event_date"))
        daily["event_date"] = daily["event_date"].dt.strftime("%Y-%m-%d")
        out["daily_environment"] = _records(daily.round(3))

        inc = inc.copy()
        if inc.empty:          # e.g. real-data mode: no incident source
            out["daily_incidents_by_category"], out["categories"] = [], []
        else:
            inc["event_date"] = pd.to_datetime(inc["event_date"]).dt.strftime("%Y-%m-%d")
            if days:
                inc = inc[inc["event_date"] >= daily["event_date"].min()] if not daily.empty else inc
            piv = inc.pivot_table(index="event_date", columns="category", values="incidents", aggfunc="sum",
                                  fill_value=0).reset_index().sort_values("event_date")
            out["daily_incidents_by_category"] = _records(piv)
            out["categories"] = [c for c in piv.columns if c != "event_date"]

        risk = self.store.read("gold", "zone_risk_hourly", columns=["zone_id", "hour", "risk_category"])
        if risk is not None and not risk.empty:
            r = risk if not zone_id else risk[risk["zone_id"] == zone_id]
            r = r.assign(event_date=pd.to_datetime(r["hour"], utc=True).dt.strftime("%Y-%m-%d"))
            if days and not daily.empty:
                r = r[r["event_date"] >= daily["event_date"].min()]
            rc = r.pivot_table(index="event_date", columns="risk_category", values="zone_id", aggfunc="count",
                               fill_value=0).reset_index().sort_values("event_date")
            out["daily_zone_hours_by_risk_category"] = _records(rc)
        hod = self.store.read("gold", "incident_hour_of_day")
        if hod is not None and not hod.empty:
            out["incidents_by_hour_of_day_ist"] = _records(hod.groupby("hour_of_day_ist")["incidents"].sum()
                                                           .reset_index().sort_values("hour_of_day_ist"))
        corr = self.store.read("gold", "traffic_hazard_correlation")
        if corr is not None and not corr.empty:
            out["traffic_hazard_correlation_all"] = _records(corr[corr["zone_id"] == "ALL"])
        bands = self.store.read("gold", "rainfall_disruption_bands")
        if bands is not None and not bands.empty:
            out["rainfall_disruption_bands"] = _records(bands.sort_values("rain_band"))
        out["live_trend"] = self.live_trend()
        out["real_data"] = self.real_data(zone_id, daily["event_date"].min() if days and not daily.empty else None)
        out["available"] = True
        return out

    def real_data(self, zone_id: Optional[str] = None, since: Optional[str] = None) -> dict:
        """Real-data layers (present only in a real-data root): observed daily rainfall
        (IMD gridded, NOAA gauge) vs ERA5, source-agreement report, official events."""
        z = self.store.read("gold", "real_daily_rainfall")
        if z is None or z.empty:
            return {"available": False}
        z = z.copy()
        z["date"] = pd.to_datetime(z["date"]).dt.strftime("%Y-%m-%d")
        if since:
            z = z[z["date"] >= since]
        if zone_id:
            z = z[z["zone_id"] == zone_id]
        reg = (z.groupby("date").agg(imd_mm=("imd_mm", "mean"), era5_mm=("era5_mm", "mean"),
                                     imd_max_mm=("imd_mm", "max")).reset_index().sort_values("date"))
        out = {"available": True, "scope": zone_id or "region mean of 40 zones",
               "daily_rainfall": _records(reg.round(2)),
               "imd_category_counts": {k: int(v) for k, v in z["imd_category"].value_counts().items()}}
        st = self.store.read("gold", "station_daily_rainfall")
        if st is not None and not st.empty:
            st = st.copy()
            st["date"] = pd.to_datetime(st["date"]).dt.strftime("%Y-%m-%d")
            if since:
                st = st[st["date"] >= since]
            out["gauge"] = {"station": str(st["station_name"].dropna().iloc[0]) if st["station_name"].notna().any() else None,
                            "zone_id": str(st["zone_id"].iloc[0]),
                            "daily": _records(st[["date", "gauge_mm", "imd_mm", "era5_mm"]].sort_values("date"))}
        ev = self.store.read("gold", "official_events")
        if ev is not None and not ev.empty:
            out["official_events"] = _records(ev.sort_values("began", ascending=False))
        out["validation"] = read_json(data_path(self.cfg, "reports_dir", "real_data_validation.json"))
        return out

    def live_trend(self, windows: int = 36) -> dict:
        df = self.store.read("live", "zone_risk", columns=["zone_id", "window_start", "risk_score", "assessed_at",
                                                           "rainfall_avg_mm_hr", "level_ratio_max", "reports"])
        if df is None or df.empty:
            return {"available": False, "reason": "no live zone-risk output yet (start streaming)"}
        df = df.sort_values("assessed_at").drop_duplicates(["zone_id", "window_start"], keep="last")
        g = (df.groupby("window_start").agg(mean_risk=("risk_score", "mean"), max_risk=("risk_score", "max"),
                                            rainfall_avg_mm_hr=("rainfall_avg_mm_hr", "mean"),
                                            level_ratio_max=("level_ratio_max", "max"), reports=("reports", "sum"),
                                            zones=("zone_id", "nunique"))
             .reset_index().sort_values("window_start").tail(windows))
        return {"available": True, "data_origin": "live-stream", "windows": _records(g.round(3))}

    def data_quality(self) -> dict:
        batch = read_json(data_path(self.cfg, "reports_dir", "dq_report_batch.json"))
        out: dict = {"batch": batch, "batch_available": batch is not None}
        live = self.store.read("live", "dq_metrics")
        if live is not None and not live.empty:
            live = live.sort_values("batch_id")
            tot = {k: int(live[k].sum()) for k in ("records", "valid", "rejected")}
            reasons: dict = {}
            for d in live["detail_json"].tail(500):
                for k, v in (json.loads(d).get("rejected_by_reason") or {}).items():
                    reasons[k] = reasons.get(k, 0) + int(v)
            out["live"] = {"data_origin": "live-stream", "batches": int(len(live)), **tot,
                           "rejected_pct": round(100.0 * tot["rejected"] / max(1, tot["records"]), 2),
                           "rejected_by_reason": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
                           "recent_batches": _records(live[["batch_id", "processed_at", "records", "valid",
                                                            "rejected"]].tail(40))}
        else:
            out["live"] = None
        q = self.store.read("quarantine", "stream", columns=["domain", "topic", "event_id", "reasons", "raw_value",
                                                             "processing_time"])
        if q is not None and not q.empty:
            q = q.sort_values("processing_time", ascending=False).head(15).copy()
            q["raw_value"] = q["raw_value"].str.slice(0, 220)
            out["quarantine_sample"] = _records(q)
        return out
