"""Hybrid data root: real rainfall + SYNTHETIC water level, traffic, reports, incidents.

Real mode (``data_real/``) has no verified open source for river levels,
traffic, citizen reports or incident severities, so its rule-based risk is
"Insufficient data". This builder fills exactly those gaps with the AEGIS
simulator, driven by the **real ERA5 hourly rainfall** of the same period:
the simulator's rain field (``Environment.rain``) is replaced by a lookup of
the ERA5 value for the zone and hour, so simulated rivers rise, roads slow down
and incidents occur when the real rain fell (e.g. Cyclone Michaung,
2023-12-03/05). Everything else in the simulator is unchanged.

Output (``--out-root``, default ``data_hybrid``)::

    raw/historical/weather.jsonl           REAL  ERA5 (copied from data_real)
    raw/historical/rainfall_daily.jsonl    REAL  IMD gridded + NOAA gauge (copied)
    raw/historical/official_events.jsonl   REAL  DFO + GDACS (copied)
    raw/historical/water_level.jsonl       SYNTHETIC, driven by ERA5 rainfall
    raw/historical/traffic.jsonl           SYNTHETIC, driven by ERA5 rainfall
    raw/historical/incidents.jsonl         SYNTHETIC reports + archived incidents (synthetic severity labels)
    raw/historical/manifest.json           what is real and what is synthetic
    reference/stations.json, zones.json    ERA5 virtual stations + synthetic gauges/road segments
    dataset_manifest.json                  data_mode = hybrid (read by the API / dashboard)

Every synthetic record keeps ``"synthetic": true`` and carries
``data_source = aegis-simulator-era5-driven``, ``observation_type = synthetic``;
real records keep their own provenance. No defects are injected.

    python -m producers.hybrid_generator                       # data_real -> data_hybrid
    python -m producers.hybrid_generator --seed 42 --step-minutes 10
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import shutil
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from aegis.config import PROJECT_ROOT, get_config, load_config
from aegis.geo import ZoneGrid, haversine_km
from aegis.logging_config import setup_logging
from producers import formats
from producers.data_generator import GENERATOR_VERSION, Simulator, _quantiles
from producers.world import Environment

log = setup_logging("hybrid")
HYBRID_SOURCE = "aegis-simulator-era5-driven"
REAL_FILES = ("weather.jsonl", "rainfall_daily.jsonl", "official_events.jsonl")
SYNTHETIC_FILES = {"water_level": "water_level.jsonl", "traffic": "traffic.jsonl", "incidents": "incidents.jsonl"}
SYNTHETIC_PARTS = {
    "water_level": "SYNTHETIC river gauges (simulator network); levels respond to the real ERA5 rainfall",
    "traffic": "SYNTHETIC road segments; speeds and disruption respond to the real ERA5 rainfall and simulated flooding",
    "citizen_reports": "SYNTHETIC public reports about simulated incidents",
    "archived_incidents_and_severity_labels": "SYNTHETIC incidents sampled from rain/flood-hazard rates; "
                                              "severity labels from producers.data_generator.severity_rule",
}


class HybridError(RuntimeError):
    pass


@dataclass
class ERA5RainEnvironment(Environment):
    """``Environment`` whose rain field is the real ERA5 hourly rainfall.

    ``rain(lat, lon, t)`` = ERA5 ``rainfall_mm_hr`` of the zone containing the
    point (nearest zone centre for points on the grid edge) for the hour that
    contains ``t``. ERA5 records are stamped at the *end* of their accumulation
    hour, so time ``t`` uses the record stamped ``floor(t, 1 h) + 1 h``. Hours
    without an ERA5 value (outside the imported period) count as no rain."""
    rain_by_zone: dict = field(default_factory=dict)      # zone_id -> {hour_end_epoch: mm}
    _zone_cache: dict = field(default_factory=dict)

    def _zone(self, lat: float, lon: float) -> Optional[str]:
        key = (round(lat, 5), round(lon, 5))
        if key not in self._zone_cache:
            z = self.grid.zone_for(lat, lon)
            if z is None:
                z = min(self.grid.zones(), key=lambda c: haversine_km(lat, lon, c.center[0], c.center[1])).zone_id
            self._zone_cache[key] = z
        return self._zone_cache[key]

    def rain(self, lat: float, lon: float, t: float) -> float:
        hour_end = int(math.floor(t / 3600.0)) * 3600 + 3600
        return self.rain_by_zone.get(self._zone(lat, lon), {}).get(hour_end, 0.0)


def load_era5_rain(weather_jsonl: Path) -> dict:
    """zone_id -> {hour_end_epoch: rainfall_mm_hr} from real ``wx-era5-1`` records."""
    out: dict = {}
    n = 0
    with weather_jsonl.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("schema_version") != "wx-era5-1" or r.get("synthetic") is not False:
                raise HybridError(f"{weather_jsonl} contains a record that is not a real ERA5 record: {r.get('event_id')}")
            mm = r.get("rainfall_mm_hr")
            if mm is None:
                continue
            zone = r["source_id"].replace("ERA5-", "", 1)
            t = int(datetime.fromisoformat(r["event_time"].replace("Z", "+00:00")).timestamp())
            out.setdefault(zone, {})[t] = float(mm)
            n += 1
    if n == 0:
        raise HybridError(f"no ERA5 rainfall values in {weather_jsonl}")
    return out


def _tag(ev: dict) -> dict:
    ev["data_source"] = HYBRID_SOURCE
    ev["observation_type"] = "synthetic"
    return ev


def _read_json(p: Path) -> Optional[dict]:
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# Wetness time constant of the simulator's linear-reservoir hydrology (dW/dt = rain - W/tau).
# The simulator default (6 h) suits its own sharp synthetic storm cells (35-95 mm/h). ERA5 cell
# averages are smoother (Michaung peak ~28 mm/h), and with 6 h the simulated rivers peak at only
# 0.77 x danger level during the cyclone although IMD observed >= 204.5 mm/day ("extremely heavy")
# on 22 zone-days. 24 h gives a peak of 1.39 x danger level. A SYNTHETIC-model setting, not a
# measured catchment property.
DEFAULT_WETNESS_TAU_H = 24.0


def generate_hybrid(cfg: dict, real_root: Path, out_root: Path, seed: int, step_minutes: int = 10,
                    tau_h: float = DEFAULT_WETNESS_TAU_H) -> dict:
    src = real_root / "raw" / "historical"
    real_manifest = _read_json(src / "manifest.json")
    if not real_manifest or not (src / "weather.jsonl").exists():
        raise HybridError(f"{src} has no real import (run scripts\\ingest_real_data.ps1 first)")
    period = real_manifest.get("period") or {}
    start, end = date.fromisoformat(period["start"]), date.fromisoformat(period["end"])
    t0 = datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()
    t_end = (datetime(end.year, end.month, end.day, tzinfo=timezone.utc) + timedelta(days=1)).timestamp()

    rain = load_era5_rain(src / "weather.jsonl")
    sim = Simulator(cfg, seed)
    sim.env = ERA5RainEnvironment(sim.grid, sim.network, rain_by_zone=rain, tau_h=tau_h)
    dt = step_minutes * 60.0

    out_dir = out_root / "raw" / "historical"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in REAL_FILES:                                        # real inputs, unchanged
        if (src / name).exists():
            shutil.copyfile(src / name, out_dir / name)
    tmp = {k: out_dir / (f + ".tmp") for k, f in SYNTHETIC_FILES.items()}
    files = {k: p.open("w", encoding="utf-8", newline="\n") for k, p in tmp.items()}
    counts: Counter = Counter()
    pending: list[tuple[float, int, str]] = []
    seq = 0

    def write(key: str, ev: dict) -> None:
        files[key].write(json.dumps(_tag(ev)) + "\n")
        counts[f"{key}.lines"] += 1

    t, steps = t0, 0
    try:
        while t < t_end:
            sim.env.step(t, dt)
            sim.update_zone_state(t, dt)
            for st in sim.network.water_stations:
                write("water_level", sim.water(st, t + sim.rng.uniform(0, 30)))
            for seg in sim.network.traffic_segments:
                write("traffic", sim.traffic(seg, t + sim.rng.uniform(0, 60)))
            for inc in sim.incidents(t, dt):
                write("incidents", formats.historical_incident_event(
                    sim.new_id("HI"), inc["incident_id"], inc["t"], inc["lat"], inc["lon"], inc["category"],
                    inc["severity"], inc["affected_road"], inc["affected_area_km2"]))
                counts[f"incidents.category.{inc['category']}"] += 1
                for tr, rep in sim.reports_for(inc):
                    if tr < t_end:
                        seq += 1
                        heapq.heappush(pending, (tr, seq, json.dumps(rep)))
            if sim.rng.random() < 0.10 * dt / 3600.0 * len(sim.grid.zones()) / 4:
                fr = sim.false_report(t + sim.rng.uniform(0, dt))
                if fr:
                    seq += 1
                    heapq.heappush(pending, (fr[0], seq, json.dumps(fr[1])))
            while pending and pending[0][0] <= t + dt:
                write("incidents", json.loads(heapq.heappop(pending)[2]))
                counts["incidents.public_reports"] += 1
            t += dt
            steps += 1
            if steps % 1000 == 0:
                log.info(f"hybrid generation: {steps} steps")
        for _, _, payload in pending:                          # reports whose time is still inside the period
            write("incidents", json.loads(payload))
            counts["incidents.public_reports"] += 1
    finally:
        for fh in files.values():
            fh.close()
    for k, p in tmp.items():                                       # atomic replace
        p.replace(out_dir / SYNTHETIC_FILES[k])

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "data_mode": "hybrid", "synthetic": "partial", "generated_at": generated_at,
        "period": period,
        "real_files": {n: (real_manifest.get("line_counts") or {}).get(n) for n in REAL_FILES if (out_dir / n).exists()},
        "synthetic_files": {SYNTHETIC_FILES[k]: counts[f"{k}.lines"] for k in SYNTHETIC_FILES},
        "synthetic_generator": {"generator": GENERATOR_VERSION, "seed": seed, "step_minutes": step_minutes,
                                "rain_field": "real ERA5 hourly rainfall per zone (data_real/raw/historical/weather.jsonl)",
                                "wetness_tau_h": tau_h, "data_source": HYBRID_SOURCE, "injected_defects": "none"},
        "flood_severity_label_counts": dict(sorted(sim.label_counts.items())),
        "flood_severity_latent_quantiles": _quantiles(sim.label_scores),
        "counts": dict(sorted(counts.items())),
        "label_rule": "producers.data_generator.severity_rule (SYNTHETIC labels; inputs driven by real ERA5 rainfall)",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    ref_out = out_root / "reference"
    ref_out.mkdir(parents=True, exist_ok=True)
    real_ref = _read_json(real_root / "reference" / "stations.json") or {}
    synth_ref = sim.network.as_reference()
    (ref_out / "stations.json").write_text(json.dumps({
        "synthetic": "partial",
        "note": "weather_stations = REAL ERA5 virtual stations (one per zone centre); water_stations and "
                "traffic_segments = SYNTHETIC simulator network (not real infrastructure)",
        "weather_stations": real_ref.get("weather_stations", []),
        "water_stations": synth_ref["water_stations"], "traffic_segments": synth_ref["traffic_segments"]},
        indent=2), encoding="utf-8")
    if (real_root / "reference" / "zones.json").exists():
        shutil.copyfile(real_root / "reference" / "zones.json", ref_out / "zones.json")

    real_ds = _read_json(real_root / "dataset_manifest.json") or {}
    not_available = {k: v for k, v in (real_ds.get("not_available") or {}).items()
                     if k == "hourly_rain_gauge_observations"}
    dataset = {
        "data_mode": "hybrid", "synthetic": "partial", "generated_at": generated_at, "period": period,
        "sources": {**(real_ds.get("sources") or {}),
                    "simulator": {"data_source": HYBRID_SOURCE, "observation_type": "synthetic", "licence": "project code",
                                  "note": "AEGIS simulator driven by the real ERA5 rainfall"}},
        "real_parts": {
            "hourly_rainfall_temperature_humidity": "ERA5 reanalysis via Open-Meteo (real, model-based, not a gauge)",
            "daily_rainfall": "IMD gridded and NOAA GHCN-D gauge observations",
            "official_events": "Dartmouth Flood Observatory and GDACS",
        },
        "synthetic_parts": SYNTHETIC_PARTS,
        "not_available": not_available,
        "label": ("HYBRID data: REAL rainfall (ERA5 hourly reanalysis; IMD/NOAA observed daily) and REAL DFO/GDACS "
                  "events; water level, traffic, citizen reports, incidents and severity labels are SYNTHETIC, "
                  "simulated by the AEGIS simulator from the real ERA5 rainfall. Risk scores therefore combine real "
                  "and synthetic indicators and are a demonstration, not an assessment of what happened."),
    }
    (out_root / "dataset_manifest.json").write_text(json.dumps(dataset, indent=2), encoding="utf-8")
    log.info(f"hybrid data written to {out_root}: {manifest['synthetic_files']}, labels {manifest['flood_severity_label_counts']}")
    return manifest


def main(argv: Optional[list[str]] = None) -> int:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="Hybrid data root: real rainfall + synthetic water/traffic/reports/incidents")
    ap.add_argument("--real-root", default="data_real")
    ap.add_argument("--out-root", default="data_hybrid")
    ap.add_argument("--seed", type=int, default=cfg["generator"]["seed"])
    ap.add_argument("--step-minutes", type=int, default=cfg["generator"]["historical_step_minutes"])
    ap.add_argument("--wetness-tau-h", type=float, default=DEFAULT_WETNESS_TAU_H,
                    help="simulator wetness time constant in hours (synthetic-model setting, see module docs)")
    args = ap.parse_args(argv)
    real_root = Path(args.real_root) if Path(args.real_root).is_absolute() else PROJECT_ROOT / args.real_root
    out_root = Path(args.out_root) if Path(args.out_root).is_absolute() else PROJECT_ROOT / args.out_root
    synthetic_root = PROJECT_ROOT / load_config(environ={})["storage"]["data_root"]     # YAML default, not env
    if out_root.resolve() in (real_root.resolve(), synthetic_root.resolve()):
        print(json.dumps({"ok": False, "error": "out-root must differ from the real and the synthetic data roots"}))
        return 2
    try:
        m = generate_hybrid(cfg, real_root, out_root, args.seed, args.step_minutes, args.wetness_tau_h)
    except (HybridError, KeyError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps({"ok": True, "out_root": str(out_root), "period": m["period"],
                      "synthetic_files": m["synthetic_files"],
                      "flood_severity_label_counts": m["flood_severity_label_counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
