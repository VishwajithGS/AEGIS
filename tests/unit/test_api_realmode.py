"""API in real-historical mode: dataset labelling, real-data analytics, no false
'unreachable' issues for live services that are deliberately off."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient


def _write(df: pd.DataFrame, path, schema=None) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), path / "part-0.parquet")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    root = tmp_path / "data_real"
    lake = root / "lake"
    (root).mkdir(parents=True)
    (root / "dataset_manifest.json").write_text(json.dumps({
        "data_mode": "real-historical", "synthetic": False, "period": {"start": "2023-11-01", "end": "2023-12-30"},
        "label": "Real historical data (test).", "not_available": {"traffic": "no verified open source"}}), encoding="utf-8")
    _write(pd.DataFrame({"zone_id": ["Z-R03-C01"], "event_date": [date(2023, 12, 4)], "rain_mm": [195.0],
                         "rain_peak_mm_hr": [28.0], "level_ratio_peak": [None], "disrupted_ratio_avg": [None],
                         "reports": [None], "archived_incidents": [0], "risk_peak": [None], "risk_avg": [None]}),
           lake / "gold" / "zone_daily")
    _write(pd.DataFrame({"event_date": pd.Series([], dtype="datetime64[ns]"), "category": pd.Series([], dtype=str),
                         "source_type": pd.Series([], dtype=str), "incidents": pd.Series([], dtype="int64"),
                         "high_or_critical": pd.Series([], dtype="int64")}), lake / "gold" / "incident_daily")
    _write(pd.DataFrame({"zone_id": ["Z-R03-C01", "Z-R03-C02"], "date": [date(2023, 12, 4)] * 2,
                         "imd_mm": [244.0, 230.0], "imd_category": ["extremely_heavy", "extremely_heavy"],
                         "imd_cell": ["IMD-13.00N-80.00E"] * 2, "cell_distance_km": [5.0, 8.0],
                         "era5_mm": [181.4, 181.4], "era5_hours": [24, 24]}), lake / "gold" / "real_daily_rainfall")
    _write(pd.DataFrame({"event_id": ["EVT-1"], "name": ["Tropical Cyclone MICHAUNG-23"], "data_source": ["gdacs"],
                         "began": ["2023-12-03T00:00:00Z"], "alert_level": ["Orange"], "severity_label": [None],
                         "spatial_scope": ["event point"]}), lake / "gold" / "official_events")
    monkeypatch.setenv("AEGIS__STORAGE__DATA_ROOT", str(root))
    monkeypatch.setenv("AEGIS__HBASE__ENABLED", "false")
    monkeypatch.setenv("AEGIS__KAFKA__BOOTSTRAP_SERVERS", "127.0.0.1:1")
    monkeypatch.setenv("AEGIS_API_LIVE_FEED", "0")
    monkeypatch.setenv("AEGIS_API_MODEL", "0")
    from aegis import config
    from backend.app import settings
    config.get_config.cache_clear()
    settings.get_settings.cache_clear()
    from backend.app.main import app
    with TestClient(app) as c:
        yield c
    config.get_config.cache_clear()
    settings.get_settings.cache_clear()


def test_dataset_endpoint_reports_real_mode(client):
    d = client.get("/api/dataset").json()
    assert d["data_mode"] == "real-historical" and d["synthetic"] is False
    assert "traffic" in d["not_available"]
    o = client.get("/api/overview").json()
    assert o["dataset"]["data_mode"] == "real-historical" and "not live" in o["data_origin_note"]


def test_real_analytics_and_empty_incident_tables(client):
    h = client.get("/api/analytics/historical").json()
    assert h["available"] and h["daily_incidents_by_category"] == [] and h["categories"] == []
    rd = h["real_data"]
    assert rd["available"] and rd["daily_rainfall"][0]["imd_mm"] == pytest.approx(237.0)
    assert rd["official_events"][0]["alert_level"] == "Orange"


def test_pipeline_and_zones_do_not_flag_disabled_live_services(client):
    p = client.get("/api/pipeline/status").json()
    assert p["data_mode"] == "real-historical" and p["issues"] == [] and p["overall"] == "ok"
    z = client.get("/api/zones").json()
    assert z["source_errors"] == {}
