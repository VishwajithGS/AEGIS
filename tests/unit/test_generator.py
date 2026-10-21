import json
import random
from datetime import datetime

import pytest

from producers import formats
from producers.anomalies import AnomalyInjector
from producers.data_generator import SEVERITY_THRESHOLDS, LiveGenerator, generate_historical, severity_rule


def _small(cfg, out, seed=7):
    end = datetime(2026, 10, 15, tzinfo=formats.IST)
    return generate_historical(cfg, out, seed, days=1, step_minutes=60, end=end, anomaly_rate=0.2,
                               reference_dir=out / "ref")


def test_historical_generation_is_deterministic(cfg, tmp_path):
    m1 = _small(cfg, tmp_path / "a")
    m2 = _small(cfg, tmp_path / "b")
    for f in ("weather", "water_level", "traffic", "incidents"):
        assert (tmp_path / "a" / f"{f}.jsonl").read_bytes() == (tmp_path / "b" / f"{f}.jsonl").read_bytes()
    assert m1["injected_anomalies"] == m2["injected_anomalies"]
    assert m1["synthetic"] is True
    assert sum(m1["line_counts"].values()) > 0
    stations = json.loads((tmp_path / "a" / "ref" / "stations.json").read_text())
    assert stations["synthetic"] is True and len(stations["weather_stations"]) == cfg["generator"]["weather_stations"]


def test_every_valid_line_is_marked_synthetic(cfg, tmp_path):
    _small(cfg, tmp_path / "a")
    n = 0
    for line in (tmp_path / "a" / "weather.jsonl").read_text().splitlines():
        try:
            ev = json.loads(line)
        except ValueError:
            continue                                   # injected malformed line
        assert ev.get("synthetic") is True
        n += 1
    assert n > 0


def test_anomaly_injector_kinds():
    ev = {"event_id": "WX-1", "source_type": "weather", "source_id": "WXM-01", "schema_version": "wx-metric-1",
          "synthetic": True, "event_time": "2026-10-20T10:00:00+05:30", "lat": 13.0, "lon": 80.2,
          "temperature_c": 27.0, "humidity_pct": 90.0, "rainfall_mm_hr": 5.0, "condition": "Rain"}
    inj = AnomalyInjector(random.Random(1))
    with pytest.raises(ValueError):
        json.loads(inj.apply(ev, "malformed_json"))
    assert json.loads(inj.apply(ev, "invalid_coordinates"))["lat"] == 95.5
    assert json.loads(inj.apply(ev, "outside_study_region"))["lat"] == 14.2
    assert json.loads(inj.apply(ev, "unknown_schema_version"))["schema_version"].endswith("-beta")
    assert isinstance(json.loads(inj.apply(ev, "invalid_type"))["rainfall_mm_hr"], str)
    missing = json.loads(inj.apply(ev, "missing_required"))
    assert len(missing) == len(ev) - 1
    assert json.loads(inj.apply(ev, "duplicate")) == ev
    assert ev["lat"] == 13.0                             # original untouched
    assert sum(inj.counts.values()) == 7


def test_severity_rule_thresholds():
    lo, mid, hi = SEVERITY_THRESHOLDS
    assert severity_rule(0, 0, 0, 0, 0)[0] == "Low"
    assert severity_rule(60, 1.0, 4, 1.0, 0)[0] == "Critical"
    label, s = severity_rule(30, 0.5, 2, 0.5, 0)
    assert s == pytest.approx(0.4 * 0.5 + 0.35 * 0.5 + 0.15 * 0.5 + 0.1 * 0.5)
    assert label == ("Low" if s < lo else "Moderate" if s < mid else "High" if s < hi else "Critical")


def test_live_generator_stamps_wall_clock_time(cfg):
    clock = {"t": 1_792_000_000.0}

    def fake_clock():
        return clock["t"]

    def fake_sleep(s):
        clock["t"] += max(s, 0.05)

    gen = LiveGenerator(cfg, seed=3, rate_per_sec=50, anomaly_rate=0.0, late_rate=0.0, clock=fake_clock)
    events = list(gen.events(duration_sec=5, sleep=fake_sleep))
    assert len(events) > 50
    for e in events:
        assert abs(e.event_time - clock["t"]) < 10
        ev = json.loads(e.value)
        if ev["schema_version"] == "wx-imperial-2":
            assert abs(ev["ts_ms"] / 1000 - e.event_time) < 10
    assert {e.topic_key for e in events} >= {"weather", "water_level", "traffic"}


def test_report_jitter_never_leaves_study_region(cfg):
    """Reports about incidents at the region's corners stay inside it; only injected
    defects may produce outside_study_region (keeps the manifest counts exact)."""
    from producers.data_generator import Simulator
    sim = Simulator(cfg, seed=5)
    r = cfg["region"]
    corners = [(r["min_lat"], r["min_lon"]), (r["min_lat"], r["max_lon"]),
               (r["max_lat"], r["min_lon"]), (r["max_lat"], r["max_lon"])]
    seen = 0
    for _ in range(200):
        for lat, lon in corners:
            inc = {"incident_id": "INC-T", "t": 1_792_000_000.0, "lat": lat, "lon": lon, "category": "flood"}
            for _, ev in sim.reports_for(inc):
                if ev["lat"] is None:
                    continue
                seen += 1
                assert r["min_lat"] <= ev["lat"] <= r["max_lat"] and r["min_lon"] <= ev["lon"] <= r["max_lon"]
    assert seen > 100
