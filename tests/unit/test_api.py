"""API tests against a tiny temporary lake (test fixtures, not pipeline output).
HBase points at a closed port, so these also verify graceful degradation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient


def _write(df: pd.DataFrame, path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path / "part-0.parquet")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    lake = tmp_path / "data" / "lake"
    t0 = datetime(2026, 10, 19, 10, tzinfo=timezone.utc)
    _write(pd.DataFrame({
        "zone_id": ["Z-R00-C00", "Z-R01-C01", "Z-R00-C00"], "hour": [t0, t0, t0 - timedelta(hours=1)],
        "risk_score": [62.5, None, 10.0], "risk_category": ["High", "Insufficient data", "Low"],
        "confidence": ["medium", "insufficient", "high"], "coverage": [0.75, 0.1, 1.0],
        "explanation": ["Score 62.5", "Insufficient data", "Score 10"], "contributions": ['{"rainfall": 30}', "{}", "{}"],
        "rainfall_avg_mm_hr": [30.0, None, 1.0], "level_ratio_max": [0.9, None, 0.5],
    }), lake / "gold" / "zone_risk_hourly")
    _write(pd.DataFrame({
        "incident_id": ["INC-1", "PR-1", "PR-2"], "event_id": ["HI-1", "PR-1", "PR-2"],
        "source_type": ["historical_incident", "public_report", "public_report"],
        "source_id": ["arch", "citizen-app", "citizen-sms"], "category": ["flood", "waterlogging", "tree_fall"],
        "category_raw": ["flood", "Water logging", "Tree fall"], "severity": ["High", None, None],
        "description": ["Archived flood", "Knee-deep water", "Tree down"], "channel": [None, "app", "sms"],
        "verification_status": ["verified", "unverified", "rejected"], "confidence": [None, 0.7, 0.2],
        "is_flood_related": [True, True, False], "lat": [13.05, 13.051, None], "lon": [80.22, 80.221, None],
        "zone_id": ["Z-R04-C02", "Z-R04-C02", None], "event_time": [t0, t0 + timedelta(minutes=5), t0],
    }), lake / "silver" / "incidents")
    monkeypatch.setenv("AEGIS__STORAGE__DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("AEGIS__HBASE__HOST", "127.0.0.1")
    monkeypatch.setenv("AEGIS__HBASE__PORT", "1")
    monkeypatch.setenv("AEGIS__KAFKA__BOOTSTRAP_SERVERS", "127.0.0.1:1")
    monkeypatch.setenv("AEGIS_API_LIVE_FEED", "0")
    from aegis import config
    from backend.app import settings
    config.get_config.cache_clear()
    settings.get_settings.cache_clear()
    from backend.app.main import app
    with TestClient(app) as c:
        yield c
    config.get_config.cache_clear()
    settings.get_settings.cache_clear()


def test_health_reports_unavailable_hbase(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["dependencies"]["hbase"] is False


def test_zones_fall_back_to_batch_and_flag_origin(client):
    body = client.get("/api/zones").json()
    assert body["count"] == 40 and "hbase" in body["source_errors"]
    first = body["zones"][0]
    assert first["zone_id"] == "Z-R00-C00"
    a = first["assessment"]
    assert a["risk_score"] == 62.5 and a["data_origin"] == "batch-historical" and a["stale"] is True
    z11 = next(z for z in body["zones"] if z["zone_id"] == "Z-R01-C01")
    assert z11["assessment"]["risk_category"] == "Insufficient data"
    assert body["zones"][-1]["assessment"] is None          # zones without any assessment sort last
    only_high = client.get("/api/zones", params={"category": "High"}).json()
    assert [z["zone_id"] for z in only_high["zones"]] == ["Z-R00-C00"]


def test_zone_detail_and_unknown_zone(client):
    assert client.get("/api/zones/Z-R99-C99").status_code == 404
    d = client.get("/api/zones/Z-R00-C00").json()
    assert d["hbase_error"] and len(d["batch_history_72h"]) == 2


def test_incident_filters_and_validation(client):
    assert client.get("/api/incidents").json()["total"] == 3
    assert client.get("/api/incidents", params={"category": "flood"}).json()["total"] == 1
    assert client.get("/api/incidents", params={"verification": "rejected"}).json()["total"] == 1
    assert client.get("/api/incidents", params={"q": "knee"}).json()["items"][0]["incident_id"] == "PR-1"
    assert client.get("/api/incidents", params={"category": "volcano"}).status_code == 422
    assert client.get("/api/incidents", params={"limit": 0}).status_code == 422


def test_nearby_uses_haversine_and_excludes_rejected(client):
    r = client.get("/api/incidents/nearby", params={"lat": 13.05, "lon": 80.22, "radius_km": 1}).json()
    assert r["total"] == 2 and r["unit"] == "km"
    assert r["items"][0]["incident_id"] == "INC-1" and r["items"][0]["distance_km"] == 0
    assert 0.1 < r["items"][1]["distance_km"] < 0.2
    assert client.get("/api/incidents/nearby", params={"lat": 95, "lon": 80}).status_code == 422


def test_model_unavailable_returns_503(client):
    s = client.get("/api/model/status").json()
    assert s["available"] is False
    body = {"reports_6h": 1, "water_obs_available": 0, "traffic_obs_available": 0}
    assert client.post("/api/model/predict", json=body).status_code == 503
    assert client.post("/api/model/predict", json={**body, "severity": "High"}).status_code == 422


def test_pipeline_status_reports_degraded(client):
    s = client.get("/api/pipeline/status").json()
    assert s["overall"] in ("degraded", "down")
    assert s["services"]["kafka"]["available"] is False
    assert any("HBase" in i for i in s["issues"])
