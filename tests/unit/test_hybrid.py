"""Hybrid data root: synthetic water/traffic/reports/incidents driven by real ERA5
rainfall. Uses the real ERA5 excerpt in tests/fixtures/real (2 zones, 2023-12-02..05)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aegis.realdata import era5
from producers.hybrid_generator import (HYBRID_SOURCE, ERA5RainEnvironment, HybridError, generate_hybrid,
                                        load_era5_rain)

FIX = Path(__file__).resolve().parent.parent / "fixtures" / "real"
ERA5 = FIX / "era5_weather_2zones_2023-12-02_05.jsonl"


def _real_root(tmp_path: Path) -> Path:
    root = tmp_path / "data_real"
    hist = root / "raw" / "historical"
    hist.mkdir(parents=True)
    (hist / "weather.jsonl").write_bytes(ERA5.read_bytes())
    (hist / "official_events.jsonl").write_bytes((FIX / "official_events_2023-11_12.jsonl").read_bytes())
    (hist / "manifest.json").write_text(json.dumps({"data_mode": "real-historical", "synthetic": False,
                                                    "period": {"start": "2023-12-02", "end": "2023-12-05"},
                                                    "line_counts": {"weather.jsonl": 192}}), encoding="utf-8")
    recs = [json.loads(l) for l in ERA5.read_text(encoding="utf-8").splitlines()]
    (root / "reference").mkdir()
    (root / "reference" / "stations.json").write_text(json.dumps(
        {"weather_stations": era5.virtual_stations(recs), "water_stations": [], "traffic_segments": []}), encoding="utf-8")
    (root / "dataset_manifest.json").write_text(json.dumps(
        {"data_mode": "real-historical", "sources": {"era5": {"data_source": "era5-open-meteo"}},
         "not_available": {"traffic": "x", "hourly_rain_gauge_observations": "ERA5 only"}}), encoding="utf-8")
    return root


def _lines(p: Path) -> list[dict]:
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def test_rain_field_is_the_era5_value_of_the_zone_and_hour(cfg):
    from producers.data_generator import Simulator
    rain = load_era5_rain(ERA5)
    sim = Simulator(cfg, 42)
    env = ERA5RainEnvironment(sim.grid, sim.network, rain_by_zone=rain)
    rec = max(_lines(ERA5), key=lambda r: r["rainfall_mm_hr"])          # wettest hour in the excerpt
    t_end = datetime.fromisoformat(rec["event_time"].replace("Z", "+00:00")).timestamp()
    zone = sim.grid.get(rec["source_id"].replace("ERA5-", ""))
    lat, lon = zone.center
    assert env.rain(lat, lon, t_end - 1800) == rec["rainfall_mm_hr"] > 10       # inside the accumulation hour
    assert env.rain(lat, lon, t_end - 3600) == rain[zone.zone_id][int(t_end)]    # hour start belongs to it too
    other = next(z for z in sim.grid.zones() if z.zone_id not in rain)
    assert env.rain(*other.center, t_end - 1800) == 0.0                         # no ERA5 value -> no rain


def test_generate_hybrid_tags_synthetic_records_and_keeps_real_files(tmp_path, cfg):
    real = _real_root(tmp_path)
    out = tmp_path / "data_hybrid"
    m = generate_hybrid(cfg, real, out, seed=42, step_minutes=30)
    hist = out / "raw" / "historical"
    assert (hist / "weather.jsonl").read_bytes() == ERA5.read_bytes()        # real ERA5, unchanged
    assert (hist / "official_events.jsonl").exists()
    for name in ("water_level.jsonl", "traffic.jsonl", "incidents.jsonl"):
        recs = _lines(hist / name)
        assert recs, name
        assert all(r["synthetic"] is True and r["data_source"] == HYBRID_SOURCE
                   and r["observation_type"] == "synthetic" for r in recs), name
    water = _lines(hist / "water_level.jsonl")
    assert min(r["event_time"] for r in water) >= "2023-12-02" and max(r["event_time"] for r in water) < "2023-12-06"
    assert m["data_mode"] == "hybrid" and "injected_anomalies" not in m        # no injected defects
    ds = json.loads((out / "dataset_manifest.json").read_text(encoding="utf-8"))
    assert ds["data_mode"] == "hybrid" and "water_level" in ds["synthetic_parts"] and ds["real_parts"]
    assert ds["not_available"] == {"hourly_rain_gauge_observations": "ERA5 only"}  # filled gaps are no longer listed
    st = json.loads((out / "reference" / "stations.json").read_text(encoding="utf-8"))
    assert {s["station_id"] for s in st["weather_stations"]} == {"ERA5-Z-R03-C01", "ERA5-Z-R03-C02"}
    assert st["water_stations"] and st["traffic_segments"]


def test_generate_hybrid_is_deterministic(tmp_path, cfg):
    real = _real_root(tmp_path)
    a, b = tmp_path / "a", tmp_path / "b"
    generate_hybrid(cfg, real, a, seed=7, step_minutes=60)
    generate_hybrid(cfg, real, b, seed=7, step_minutes=60)
    for name in ("water_level.jsonl", "traffic.jsonl", "incidents.jsonl"):
        assert (a / "raw" / "historical" / name).read_bytes() == (b / "raw" / "historical" / name).read_bytes()


def test_hybrid_refuses_non_era5_weather(tmp_path, cfg):
    real = _real_root(tmp_path)
    w = real / "raw" / "historical" / "weather.jsonl"
    rec = _lines(w)[0] | {"synthetic": True, "schema_version": "wx-metric-1"}
    w.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    with pytest.raises(HybridError, match="not a real ERA5 record"):
        generate_hybrid(cfg, real, tmp_path / "out", seed=1, step_minutes=60)
    with pytest.raises(HybridError, match="no real import"):
        generate_hybrid(cfg, tmp_path / "missing", tmp_path / "out2", seed=1)


def test_dataset_service_labels_hybrid_mode(tmp_path):
    from backend.app.services.dataset_service import DatasetService
    root = tmp_path / "data_hybrid"
    root.mkdir()
    (root / "dataset_manifest.json").write_text(json.dumps({"data_mode": "hybrid", "synthetic": "partial"}),
                                                encoding="utf-8")
    svc = DatasetService({"storage": {"data_root": str(root), "generated_dir": "generated"}})
    assert svc.info()["data_mode"] == "hybrid"
    assert "SYNTHETIC" in svc.origin_note()
