"""HTTP endpoints. Business logic lives in backend/app/services."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from aegis.ml_contract import FeatureValidationError
from backend.app.api.deps import Services, services
from backend.app.schemas import PredictRequest

router = APIRouter()
CATEGORIES = ["flood", "waterlogging", "tree_fall", "power_outage", "road_blockage", "structure_damage", "other"]


@router.get("/health", tags=["system"])
def health(s: Services = Depends(services)) -> dict:
    """Liveness plus a quick reachability summary of dependencies."""
    hb = s.hbase.health()
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat(),
            "dependencies": {"hbase": hb.get("available"), "kafka_live_feed": s.feed.status.get("connected"),
                             "model": s.model.state}}


@router.get("/api/overview", tags=["dashboard"])
def overview(s: Services = Depends(services)) -> dict:
    zones = s.zones.current()
    inc = s.incidents.summary()
    pipe = s.pipeline.streaming()
    dq = s.analytics.data_quality()
    live_dq = dq.get("live") or {}
    batch_dq = (dq.get("batch") or {}).get("totals") or {}
    counts = {
        "stream_events_archived": s.store.count("bronze", "kafka_raw"),
        "stream_events_processed": sum(s.store.count("silver_stream", d) for d in ("weather", "water_level", "traffic", "incidents")),
        "batch_records_processed": batch_dq.get("records"),
        "batch_silver_rows": batch_dq.get("silver_rows"),
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts, "incidents": inc,
        "zones": {"total": len(zones["zones"]), "category_counts": zones["category_counts"],
                  "live_zones": zones["live_zones"], "fresh_live_zones": zones["fresh_live_zones"],
                  "latest_window_end": zones["latest_window_end"]},
        "data_quality": {"live_rejected_pct": live_dq.get("rejected_pct"), "live_records": live_dq.get("records"),
                         "live_rejected": live_dq.get("rejected"),
                         "batch_rejected_pct": round(100 * batch_dq["rejected"] / batch_dq["records"], 2)
                         if batch_dq.get("records") else None},
        "processing": {"streaming_running": pipe.get("running", False), "streaming_state": pipe.get("state"),
                       "hbase_sink": pipe.get("hbase_sink"), "updated_at": pipe.get("updated_at")},
        "model": {"state": s.model.state, "version": s.model.latest_version()},
        "alerts": len(s.feed.recent_alerts()),
        "data_origin_note": "live-stream = Structured Streaming output; batch-historical = batch ETL over the "
                            "synthetic 60-day backfill.",
    }


@router.get("/api/zones", tags=["zones"])
def zones(category: Optional[str] = Query(None, description="filter by risk category"),
          sort: str = Query("risk_score", pattern="^(risk_score|zone_id|coverage)$"),
          order: str = Query("desc", pattern="^(asc|desc)$"),
          s: Services = Depends(services)) -> dict:
    data = s.zones.current()
    items = data["zones"]
    if category:
        items = [z for z in items if (z["assessment"] or {}).get("risk_category") == category]

    desc = order == "desc"
    if sort == "zone_id":
        items = sorted(items, key=lambda z: z["zone_id"], reverse=desc)
    else:   # zones without a value (e.g. "Insufficient data") always go last
        has = [z for z in items if (z["assessment"] or {}).get(sort) is not None]
        rest = [z for z in items if (z["assessment"] or {}).get(sort) is None]
        items = sorted(has, key=lambda z: z["assessment"][sort], reverse=desc) + rest
    return {**data, "zones": items, "count": len(items)}


@router.get("/api/zones/{zone_id}", tags=["zones"])
def zone_detail(zone_id: str, s: Services = Depends(services)) -> dict:
    d = s.zones.detail(zone_id)
    if d is None:
        raise HTTPException(404, f"unknown zone {zone_id}")
    return d


@router.get("/api/incidents", tags=["incidents"])
def incidents(category: Optional[str] = Query(None), source_type: Optional[str] = Query(None, pattern="^(public_report|historical_incident)$"),
              verification: Optional[str] = Query(None, pattern="^(verified|unverified|rejected)$"),
              zone_id: Optional[str] = Query(None, max_length=32),
              origin: Optional[str] = Query(None, pattern="^(live-stream|batch-historical)$"),
              q: Optional[str] = Query(None, max_length=80), since_hours: Optional[float] = Query(None, gt=0, le=24 * 400),
              located_only: bool = False, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
              s: Services = Depends(services)) -> dict:
    if category and category not in CATEGORIES:
        raise HTTPException(422, f"category must be one of {CATEGORIES}")
    return s.incidents.list(category, source_type, verification, zone_id, origin, q, since_hours, located_only,
                            limit, offset)


@router.get("/api/incidents/nearby", tags=["incidents"])
def nearby(lat: float = Query(..., ge=-90, le=90), lon: float = Query(..., ge=-180, le=180),
           radius_km: float = Query(2.0, gt=0, le=50), limit: int = Query(100, ge=1, le=500),
           origin: Optional[str] = Query(None, pattern="^(live-stream|batch-historical)$"),
           s: Services = Depends(services)) -> dict:
    return s.incidents.nearby(lat, lon, radius_km, limit, origin)


@router.get("/api/incidents/{incident_id}", tags=["incidents"])
def incident(incident_id: str, s: Services = Depends(services)) -> dict:
    r = s.incidents.get(incident_id)
    if r is None:
        raise HTTPException(404, f"incident {incident_id} not found")
    return r


@router.get("/api/analytics/historical", tags=["analytics"])
def historical(zone_id: Optional[str] = Query(None, max_length=32), days: Optional[int] = Query(None, ge=1, le=400),
               s: Services = Depends(services)) -> dict:
    return s.analytics.historical(zone_id, days)


@router.get("/api/analytics/data-quality", tags=["analytics"])
def data_quality(s: Services = Depends(services)) -> dict:
    return s.analytics.data_quality()


@router.get("/api/pipeline/status", tags=["pipeline"])
def pipeline_status(s: Services = Depends(services)) -> dict:
    return s.pipeline.status()


@router.get("/api/alerts", tags=["alerts"])
def alerts(limit: int = Query(50, ge=1, le=200), s: Services = Depends(services)) -> dict:
    current = [{"zone_id": z["zone_id"], **{k: z["assessment"].get(k) for k in
                                            ("risk_category", "risk_score", "window_end", "stale", "data_origin")}}
               for z in s.zones.current()["zones"]
               if z["assessment"] and z["assessment"].get("risk_category") in s.settings.cfg["alerts"]["categories"]]
    return {"alerts": s.feed.recent_alerts(limit), "current_high_risk_zones": current,
            "rule": f"category in {s.settings.cfg['alerts']['categories']}",
            "delivery": "dashboard only - AEGIS does not notify any external or emergency service"}


@router.get("/api/model/status", tags=["model"])
def model_status(s: Services = Depends(services)) -> dict:
    return s.model.status()


@router.post("/api/model/predict", tags=["model"], responses={503: {"description": "model not available"}})
def model_predict(req: PredictRequest, s: Services = Depends(services)) -> dict:
    if s.model.state != "ready":
        raise HTTPException(503, f"model unavailable: {s.model.reason or s.model.state}")
    try:
        return s.model.predict(req.model_dump())
    except FeatureValidationError as exc:
        raise HTTPException(422, "; ".join(exc.errors))
    except RuntimeError as exc:
        raise HTTPException(503, f"model unavailable: {exc}")


@router.get("/api/model/predictions", tags=["model"])
def model_predictions(s: Services = Depends(services)) -> dict:
    """Latest per-zone predictions from the inference job plus recent API predictions (HBase)."""
    from backend.app.services.parquet_store import read_json
    from aegis.config import data_path
    zone = read_json(data_path(s.settings.cfg, "state_dir", "ml_zone_predictions_latest.json"))
    recent, err = [], None
    try:
        recent = s.hbase.recent_predictions("api", 20)
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
    return {"zone_predictions": zone, "recent_api_predictions": recent, "hbase_error": err}


@router.get("/api/stream", tags=["live"])
async def stream(request: Request, since: int = Query(0, ge=0), s: Services = Depends(services)):
    """Server-Sent Events: zone_risk, data_quality and alert events from Kafka."""
    async def gen():
        last = since
        yield f"event: status\ndata: {json.dumps(s.feed.status)}\n\n"
        idle = 0
        while not await request.is_disconnected():
            events = s.feed.since(last)
            for e in events:
                last = e["seq"]
                yield f"id: {e['seq']}\nevent: {e['type']}\ndata: {json.dumps(e['data'], default=str)}\n\n"
            idle = 0 if events else idle + 1
            if idle >= 15:
                idle = 0
                yield f"event: status\ndata: {json.dumps(s.feed.status)}\n\n"
            await asyncio.sleep(1.0)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
