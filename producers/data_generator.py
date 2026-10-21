"""Deterministic synthetic data generator for AEGIS.

Two modes share one world model (``producers/world.py``):

* **historical** - writes N days of backfill as JSON-lines files (one raw
  payload per line, exactly as it would appear on Kafka) plus a manifest and
  the synthetic sensor registry. Used by the batch ETL, RDD/SQL demos and the
  ML training set.
* **live** - an iterator of events stamped with the current wall-clock time,
  paced to a target rate, consumed by the Kafka producers.

ALL OUTPUT IS SYNTHETIC. Severity labels on archived incidents are produced by
the documented rule in ``severity_rule`` - they are not real outcomes.

CLI::

    python -m producers.data_generator historical --days 30 --seed 42
    python -m producers.data_generator sample --out data/sample   # small demo set
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import random
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional

from aegis.config import data_path, get_config
from aegis.geo import ZoneGrid, is_on_land
from aegis.logging_config import setup_logging
from producers import formats
from producers.anomalies import FIELD_ANOMALIES, LATE_KINDS, TIME_ANOMALIES, AnomalyInjector, pick
from producers.world import Environment, SensorNetwork, historical_storms, live_storms

GENERATOR_VERSION = "aegis-sim-1.0"
TOPIC_KEYS = ("weather", "water_level", "traffic", "incidents")

# Hourly incident rates per zone: rate = base + a * rain_n^2 + b * hazard^2
FLOOD_RATE = (0.008, 0.90, 1.60)
OTHER_RATES = {
    "tree_fall": (0.002, 0.030, 0.0),
    "power_outage": (0.002, 0.030, 0.0),
    "road_blockage": (0.001, 0.0, 0.05),
    "structure_damage": (0.0005, 0.0, 0.012),
}
SEVERITY_THRESHOLDS = (0.28, 0.60, 0.84)   # -> Low | Moderate | High | Critical (~p35/p65/p87 of the latent score)


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def severity_rule(rain_3h_mm: float, hazard: float, recent_incidents: int, traffic_disruption: float,
                  noise: float) -> tuple[str, float]:
    """Synthetic label rule for flood-related incidents (documented in docs/ml_model.md).

    latent = 0.40*clamp(rain_3h/60) + 0.35*hazard + 0.15*clamp(recent/4)
             + 0.10*traffic_disruption + noise,   noise ~ N(0, 0.07)

    Inputs are the simulator's TRUE latent conditions; the ML model only sees
    noisy, partial sensor observations of them produced by the ETL.
    """
    s = (0.40 * clamp01(rain_3h_mm / 60.0) + 0.35 * clamp01(hazard)
         + 0.15 * clamp01(recent_incidents / 4.0) + 0.10 * clamp01(traffic_disruption) + noise)
    lo, mid, hi = SEVERITY_THRESHOLDS
    label = "Low" if s < lo else "Moderate" if s < mid else "High" if s < hi else "Critical"
    return label, s


def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    l, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= l:
            return k
        k += 1


@dataclass
class Emitted:
    topic_key: str
    key: str
    value: str
    event_time: float
    kind: str = "valid"


class Simulator:
    """Shared observation logic for historical and live generation."""

    def __init__(self, cfg: dict, seed: int):
        self.cfg = cfg
        self.seed = seed
        self.grid = ZoneGrid.from_config(cfg)
        self.network = SensorNetwork.build(cfg, seed)
        self.rng = random.Random(seed)
        self.injector = AnomalyInjector(random.Random(seed + 1))
        self.env = Environment(self.grid, self.network)
        self.counts: Counter = Counter()
        self.zone_rain_hist: dict[str, deque] = {z.zone_id: deque() for z in self.grid.zones()}
        self.zone_incidents: dict[str, deque] = {z.zone_id: deque() for z in self.grid.zones()}
        self.zone_traffic_disruption: dict[str, float] = {}
        self.segments_by_zone: dict[str, list[dict]] = {}
        for s in self.network.traffic_segments:
            self.segments_by_zone.setdefault(self.grid.zone_for(s["lat"], s["lon"]) or "", []).append(s)
        self.label_counts: Counter = Counter()
        self.label_scores: list[float] = []

    # -- identifiers --------------------------------------------------------
    def new_id(self, prefix: str) -> str:
        return f"{prefix}-{self.rng.getrandbits(56):014x}"

    # -- observations -------------------------------------------------------
    def weather(self, st: dict, t: float) -> dict:
        rain = self.env.rain(st["lat"], st["lon"], t)
        measured = max(0.0, rain * (1 + self.rng.gauss(0, 0.08)) + self.rng.gauss(0, 0.2))
        if measured < 0.3:
            measured = 0.0
        hour = datetime.fromtimestamp(t, formats.IST).hour + datetime.fromtimestamp(t, formats.IST).minute / 60
        temp = 28.0 + 3.0 * math.cos((hour - 14.0) / 24.0 * 2 * math.pi) - 0.10 * min(rain, 40) + self.rng.gauss(0, 0.4)
        hum = min(100.0, 68.0 + 30.0 * (1 - math.exp(-rain / 6.0)) + self.rng.gauss(0, 2.5))
        return formats.weather_event(self.new_id("WX"), st, t, temp, max(5.0, hum), measured, self.rng)

    def water(self, st: dict, t: float) -> dict:
        level = self.env.river_level_m(st) + self.rng.gauss(0, 0.015)
        u = self.rng.random()
        status = "OK"
        if u < 0.01:
            status, level = self.rng.choice(["FAULT", "error", "offline"]), level * self.rng.uniform(1.3, 2.0)
        elif u < 0.04:
            status = self.rng.choice(["DEGRADED", "low_battery", "warning"])
        else:
            status = self.rng.choice(["OK", "ok", "normal"])
        return formats.water_event(self.new_id("WL"), st, t, max(0.0, level), status)

    def traffic(self, seg: dict, t: float) -> dict:
        zone = self.grid.zone_for(seg["lat"], seg["lon"])
        rain = self.env.rain(seg["lat"], seg["lon"], t)
        ratio = self.env.nearest_gauge_ratio(seg["lat"], seg["lon"], 5.0)
        flood = max(clamp01((ratio - 0.75) / 0.35), self.env.flood_hazard(zone) if zone else 0.0)
        hour = datetime.fromtimestamp(t, formats.IST).hour + datetime.fromtimestamp(t, formats.IST).minute / 60
        rush = math.exp(-((hour - 9.0) ** 2) / 2.0) + math.exp(-((hour - 18.5) ** 2) / 2.0)
        speed_ratio = max(0.03, min(1.0, 1.0 - 0.010 * rain - 0.55 * flood - 0.25 * rush + self.rng.gauss(0, 0.06)))
        density = seg["base_density"] * (1 + 0.8 * rush + 0.4 * min(rain / 30.0, 1.0)) * (1 + self.rng.gauss(0, 0.1))
        if flood > 0.55 and self.rng.random() < 0.6:
            disruption = self.rng.choice(["FLOODED", "Waterlogged", "flooded"])
        elif speed_ratio < 0.2:
            disruption = self.rng.choice(["BLOCKED", "closed", "Blocked"])
        elif speed_ratio < 0.5:
            disruption = self.rng.choice(["SLOW", "congested", "slow"])
        else:
            disruption = self.rng.choice(["NONE", "free", "none"])
        return formats.traffic_event(self.new_id("TR"), seg, t, max(0.0, density),
                                     seg["free_flow_kmph"] * speed_ratio, disruption)

    def update_zone_state(self, t: float, dt_s: float) -> None:
        """Track true per-zone rain history and traffic disruption (inputs of the label rule)."""
        horizon = t - 3 * 3600
        for z in self.grid.zones():
            r = self.env.rain(z.center[0], z.center[1], t)
            dq = self.zone_rain_hist[z.zone_id]
            dq.append((t, r * dt_s / 3600.0))
            while dq and dq[0][0] < horizon:
                dq.popleft()
            segs = self.segments_by_zone.get(z.zone_id, [])
            haz = self.env.flood_hazard(z.zone_id)
            if segs:
                vals = []
                for s in segs:
                    rain = self.env.rain(s["lat"], s["lon"], t)
                    vals.append(1.0 if (1 - 0.010 * rain - 0.55 * haz) < 0.4 else 0.0)
                self.zone_traffic_disruption[z.zone_id] = sum(vals) / len(vals)
            else:
                self.zone_traffic_disruption[z.zone_id] = clamp01(haz * 1.2)

    def random_point_in_zone(self, zone_id: str) -> Optional[tuple[float, float]]:
        z = self.grid.get(zone_id)
        if z is None:
            return None
        for _ in range(30):
            lat = self.rng.uniform(z.min_lat, z.max_lat)
            lon = self.rng.uniform(z.min_lon, z.max_lon)
            if is_on_land(lat, lon):
                return round(lat, 5), round(lon, 5)
        return None

    def incidents(self, t: float, dt_s: float) -> list[dict]:
        """Sample true incidents in every zone for one step. Returns internal
        incident dicts (not yet encoded)."""
        out = []
        dt_h = dt_s / 3600.0
        for z in self.grid.zones():
            zid = z.zone_id
            rain_n = clamp01(self.env.rain(z.center[0], z.center[1], t) / 50.0)
            haz = self.env.flood_hazard(zid)
            recent = self.zone_incidents[zid]
            while recent and recent[0] < t - 6 * 3600:
                recent.popleft()
            rates = {"flood_related": FLOOD_RATE[0] + FLOOD_RATE[1] * rain_n ** 2 + FLOOD_RATE[2] * haz ** 2}
            for cat, (b, a_r, a_h) in OTHER_RATES.items():
                rates[cat] = b + a_r * rain_n + a_h * haz
            for cat, rate in rates.items():
                for _ in range(poisson(self.rng, rate * dt_h)):
                    pt = self.random_point_in_zone(zid)
                    if pt is None:
                        continue
                    ti = t + self.rng.uniform(0, dt_s)
                    if cat == "flood_related":
                        category = "flood" if (haz > 0.35 and self.rng.random() < 0.65) else "waterlogging"
                        rain3h = sum(v for _, v in self.zone_rain_hist[zid])
                        sev, latent = severity_rule(rain3h, haz, len(recent),
                                                    self.zone_traffic_disruption.get(zid, 0.0),
                                                    self.rng.gauss(0, 0.07))
                        self.label_counts[sev] += 1
                        self.label_scores.append(latent)
                    else:
                        category = cat
                        s = 0.5 * rain_n + 0.3 * haz + self.rng.gauss(0, 0.12)
                        sev = "Low" if s < 0.25 else "Moderate" if s < 0.45 else "High" if s < 0.65 else "Critical"
                    segs = self.segments_by_zone.get(zid, [])
                    out.append({
                        "incident_id": self.new_id("INC"), "t": ti, "lat": pt[0], "lon": pt[1],
                        "zone_id": zid, "category": category, "severity": sev,
                        "affected_road": self.rng.choice(segs)["segment_id"] if segs and self.rng.random() < 0.5 else None,
                        "affected_area_km2": self.rng.uniform(0.05, 0.6) * (1 + 3 * haz),
                    })
                    recent.append(ti)
        return out

    def reports_for(self, inc: dict, delay_scale: float = 1.0) -> list[tuple[float, dict]]:
        """Citizen reports about a true incident (0-3, delayed, jittered)."""
        n = sum(1 for _ in range(3) if self.rng.random() < 0.5)
        out = []
        for _ in range(n):
            delay = self.rng.uniform(2, 40) * 60.0 * delay_scale
            tr = inc["t"] + delay
            if self.rng.random() < 0.12:
                lat = lon = None
            else:
                # ~250 m position jitter, clamped so a report about an incident inside the
                # study region never lands outside it (only *injected* defects may do that,
                # so the manifest's injected counts stay exact). No extra random draws.
                r = self.cfg["region"]
                lat = round(min(max(inc["lat"] + self.rng.gauss(0, 0.0023), r["min_lat"] + 1e-5), r["max_lat"] - 1e-5), 5)
                lon = round(min(max(inc["lon"] + self.rng.gauss(0, 0.0023), r["min_lon"] + 1e-5), r["max_lon"] - 1e-5), 5)
            cat = inc["category"]
            if cat in ("flood", "waterlogging") and self.rng.random() < 0.1:
                cat = "waterlogging" if cat == "flood" else "flood"
            u = self.rng.random()
            ver = ("verified" if u < 0.35 else "unverified" if u < 0.90 else "rejected")
            ver = self.rng.choice({"verified": ["verified", "Confirmed"], "unverified": ["unverified", "pending", "NEW"],
                                   "rejected": ["rejected", "duplicate"]}[ver])
            ev = formats.public_report_event(self.new_id("PR"), tr, lat, lon, cat,
                                             self.rng.choice(["app", "sms", "social"]), ver,
                                             self.rng.betavariate(5, 2), self.rng)
            out.append((tr, ev))
        return out

    def false_report(self, t: float) -> Optional[tuple[float, dict]]:
        z = self.rng.choice(self.grid.zones())
        pt = self.random_point_in_zone(z.zone_id)
        if pt is None:
            return None
        cat = self.rng.choice(list(formats.CATEGORY_TEXT))
        ver = self.rng.choice(["rejected", "unverified", "unverified", "spam"])
        ev = formats.public_report_event(self.new_id("PR"), t, pt[0], pt[1], cat, self.rng.choice(["app", "social"]),
                                         ver, self.rng.betavariate(2, 5), self.rng)
        return t, ev

    def maybe_defect(self, ev: dict, rate: float) -> list[tuple[str, str]]:
        """Serialize ``ev``; with probability ``rate`` inject a field-level defect.
        Returns ``[(kind, payload), ...]`` (two entries for duplicates)."""
        if self.rng.random() >= rate:
            return [("valid", json.dumps(ev))]
        kind = pick(self.rng, FIELD_ANOMALIES)
        payload = self.injector.apply(ev, kind)
        if kind == "duplicate":
            return [("valid", payload), ("duplicate", payload)]
        return [(kind, payload)]


# ---------------------------------------------------------------------------
# Historical backfill
# ---------------------------------------------------------------------------
def generate_historical(cfg: dict, out_dir: Path, seed: int, days: int, step_minutes: int,
                        end: Optional[datetime] = None, anomaly_rate: Optional[float] = None,
                        reference_dir: Optional[Path] = None) -> dict:
    log = setup_logging("generator")
    sim = Simulator(cfg, seed)
    rate = cfg["generator"]["historical_anomaly_rate"] if anomaly_rate is None else anomaly_rate
    if end is None:
        now_ist = datetime.now(formats.IST)
        end = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    t_end = end.timestamp()
    t0 = t_end - days * 86400
    dt = step_minutes * 60.0
    sim.env.storms = historical_storms(random.Random(seed + 99), cfg, t0, days)
    sim.env.warm_up(t0, hours=12)

    out_dir.mkdir(parents=True, exist_ok=True)
    files = {k: (out_dir / f"{k}.jsonl").open("w", encoding="utf-8", newline="\n") for k in TOPIC_KEYS}
    counts: Counter = Counter()
    pending_reports: list[tuple[float, int, str]] = []   # min-heap of (t, seq, payload) by report time
    seq = 0

    def write(topic_key: str, ev: dict, defect_rate: float) -> None:
        for kind, payload in sim.maybe_defect(ev, defect_rate):
            files[topic_key].write(payload + "\n")
            counts[f"{topic_key}.lines"] += 1
            counts[f"{topic_key}.{kind}"] += 1

    t = t0
    steps = 0
    while t < t_end:
        sim.env.refresh_active(t - 60.0, t + dt + 120.0)
        sim.env.step(t, dt)
        sim.update_zone_state(t, dt)
        for st in sim.network.weather_stations:
            write("weather", sim.weather(st, t + sim.rng.uniform(0, 30)), rate)
        for st in sim.network.water_stations:
            write("water_level", sim.water(st, t + sim.rng.uniform(0, 30)), rate)
        for seg in sim.network.traffic_segments:
            write("traffic", sim.traffic(seg, t + sim.rng.uniform(0, 60)), rate)
        for inc in sim.incidents(t, dt):
            hi = formats.historical_incident_event(
                sim.new_id("HI"), inc["incident_id"], inc["t"], inc["lat"], inc["lon"], inc["category"],
                inc["severity"], inc["affected_road"], inc["affected_area_km2"])
            write("incidents", hi, rate / 2)
            counts[f"incidents.category.{inc['category']}"] += 1
            for tr, rep in sim.reports_for(inc):
                if tr < t_end:
                    seq += 1
                    heapq.heappush(pending_reports, (tr, seq, json.dumps(rep)))
        if sim.rng.random() < 0.10 * dt / 3600.0 * len(sim.grid.zones()) / 4:
            fr = sim.false_report(t + sim.rng.uniform(0, dt))
            if fr:
                seq += 1
                heapq.heappush(pending_reports, (fr[0], seq, json.dumps(fr[1])))
        while pending_reports and pending_reports[0][0] <= t + dt:
            _, _, payload = heapq.heappop(pending_reports)
            write("incidents", json.loads(payload), rate)
            counts["incidents.public_reports"] += 1
        t += dt
        steps += 1
        if steps % 500 == 0:
            log.info(f"historical generation progress: {steps} steps, {sum(v for k, v in counts.items() if k.endswith('.lines'))} lines")
    for _, _, payload in pending_reports:
        write("incidents", json.loads(payload), rate)
        counts["incidents.public_reports"] += 1
    for fh in files.values():
        fh.close()

    manifest = {
        "synthetic": True,
        "generator": GENERATOR_VERSION,
        "seed": seed,
        "days": days,
        "step_minutes": step_minutes,
        "start_utc": formats.iso_utc(t0),
        "end_utc": formats.iso_utc(t_end),
        "storms": len(sim.env.storms),
        "anomaly_rate": rate,
        "files": {k: f"{k}.jsonl" for k in TOPIC_KEYS},
        "line_counts": {k: counts[f"{k}.lines"] for k in TOPIC_KEYS},
        "injected_anomalies": dict(sorted(sim.injector.counts.items())),
        "flood_severity_label_counts": dict(sorted(sim.label_counts.items())),
        "flood_severity_latent_quantiles": _quantiles(sim.label_scores),
        "counts": dict(sorted(counts.items())),
        "label_rule": "producers.data_generator.severity_rule (synthetic labels; see docs/ml_model.md)",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_reference(cfg, sim, reference_dir or data_path(cfg, "reference_dir"))
    log.info(f"historical generation complete: {manifest['line_counts']} -> {out_dir}")
    return manifest


def _quantiles(values: list[float]) -> dict:
    if not values:
        return {}
    v = sorted(values)
    return {f"p{int(q * 100)}": round(v[min(len(v) - 1, int(q * len(v)))], 4) for q in (0.1, 0.25, 0.5, 0.75, 0.9)}


def write_reference(cfg: dict, sim: Simulator, ref_dir: Path) -> None:
    """Synthetic sensor registry and zone grid (reference data for spatial joins)."""
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / "stations.json").write_text(json.dumps(sim.network.as_reference(), indent=2), encoding="utf-8")
    zones = [{"zone_id": z.zone_id, "row": z.row, "col": z.col, "min_lat": z.min_lat, "min_lon": z.min_lon,
              "max_lat": z.max_lat, "max_lon": z.max_lon, "center_lat": round(z.center[0], 6),
              "center_lon": round(z.center[1], 6)} for z in sim.grid.zones()]
    (ref_dir / "zones.json").write_text(json.dumps({
        "note": "Regular analysis grid over the demo bounding box; not official boundaries.",
        "cell_deg": sim.grid.cell, "zones": zones}, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Live stream
# ---------------------------------------------------------------------------
class LiveGenerator:
    """Paced, wall-clock-stamped event stream.

    Latent dynamics run on an accelerated simulation clock
    (``sim = wall * accel``) so a full storm passes in
    ``live_storm_cycle_minutes`` of real time, but every event is stamped with
    the real current time so Spark event-time windows and watermarks behave
    as they would with live sensors.
    """

    SENSOR_SHARE = 0.85

    def __init__(self, cfg: dict, seed: Optional[int] = None, rate_per_sec: Optional[float] = None,
                 sources: Optional[set] = None, anomaly_rate: Optional[float] = None,
                 late_rate: float = 0.03, clock=time.time):
        g = cfg["generator"]
        self.cfg = cfg
        self.seed = int(g["seed"] if seed is None else seed)
        self.sim = Simulator(cfg, self.seed)
        self.sources = set(sources or TOPIC_KEYS)
        self.rate = float(g["live_rate_per_sec"] if rate_per_sec is None else rate_per_sec)
        self.anomaly_rate = float(g["anomaly_rate"] if anomaly_rate is None else anomaly_rate)
        self.late_rate = late_rate
        self.cycle_sim_s = 6 * 3600.0
        self.accel = self.cycle_sim_s / (float(g["live_storm_cycle_minutes"]) * 60.0)
        self.counts: Counter = Counter()
        self.clock = clock
        now = clock()
        sim_now = now * self.accel
        self.sim.env.storms = live_storms(self.seed, cfg, self.cycle_sim_s, sim_now - 24 * 3600, sim_now + 4 * 3600)
        self.sim.env.warm_up(sim_now, hours=18)
        self._last_step_wall = now
        self._storm_horizon = sim_now + 4 * 3600
        self._reports: list[tuple[float, int, dict]] = []
        self._seq = 0

        sensors = []
        if "weather" in self.sources:
            sensors += [("weather", s) for s in self.sim.network.weather_stations]
        if "water_level" in self.sources:
            sensors += [("water_level", s) for s in self.sim.network.water_stations]
        if "traffic" in self.sources:
            sensors += [("traffic", s) for s in self.sim.network.traffic_segments]
        self.interval = (len(sensors) / max(0.01, self.rate * self.SENSOR_SHARE)) if sensors else 1.0
        self._heap = [(now + self.sim.rng.uniform(0, self.interval), i, kind, s)
                      for i, (kind, s) in enumerate(sensors)]
        heapq.heapify(self._heap)

    def sim_time(self, wall: float) -> float:
        return wall * self.accel

    def _advance(self, wall: float) -> None:
        dt_wall = wall - self._last_step_wall
        if dt_wall < 1.0:
            return
        sim_t = self.sim_time(wall)
        if sim_t + 3600 > self._storm_horizon:
            self.sim.env.storms = live_storms(self.seed, self.cfg, self.cycle_sim_s, sim_t - 24 * 3600, sim_t + 4 * 3600)
            self._storm_horizon = sim_t + 4 * 3600
        self.sim.env.refresh_active(sim_t - 3600.0, sim_t + 3600.0)
        self.sim.env.step(sim_t, dt_wall * self.accel)
        self.sim.update_zone_state(sim_t, dt_wall * self.accel)
        if "incidents" in self.sources:
            for inc in self.sim.incidents(sim_t, dt_wall * self.accel):
                inc_wall = wall - self.sim.rng.uniform(0, dt_wall)
                inc = dict(inc, t=inc_wall)
                self.counts["true_incidents"] += 1
                for tr, ev in self.sim.reports_for(inc, delay_scale=1.0 / self.accel):
                    self._seq += 1
                    heapq.heappush(self._reports, (tr, self._seq, ev))
            if self.sim.rng.random() < 0.004 * dt_wall * self.accel / 60.0:
                fr = self.sim.false_report(wall)
                if fr:
                    self._seq += 1
                    heapq.heappush(self._reports, (wall, self._seq, fr[1]))
        self._last_step_wall = wall

    def _encode(self, kind: str, src: dict, t_wall: float) -> dict:
        # Observation values come from the accelerated latent state; the
        # payload timestamp is the real time ``t_wall``.
        sim_t = self.sim_time(t_wall)
        if kind == "weather":
            ev = self.sim.weather(src, sim_t)
            topic_key = "weather"
        elif kind == "water_level":
            ev = self.sim.water(src, sim_t)
            topic_key = "water_level"
        else:
            ev = self.sim.traffic(src, sim_t)
            topic_key = "traffic"
        return self._restamp(ev, t_wall), topic_key

    @staticmethod
    def _restamp(ev: dict, t: float) -> dict:
        v = ev["schema_version"]
        if v == "wx-imperial-2":
            ev["ts_ms"] = int(round(t * 1000))
        elif v in ("wx-metric-1", "pr-1"):
            ev["event_time"] = formats.iso_ist(t)
        elif v in ("wl-1", "hi-1"):
            ev["event_time"] = formats.iso_utc(t)
        elif v == "tr-1":
            ev["event_time"] = formats.local_naive(t)
        return ev

    def _finalize(self, topic_key: str, ev: dict, now: float) -> list[Emitted]:
        """Apply late / future / field defects and serialize."""
        r = self.sim.rng.random()
        if r < self.late_rate:
            late = pick(self.sim.rng, LATE_KINDS)
            shift = self.sim.rng.uniform(3, 8) if late == "late_within_watermark" else self.sim.rng.uniform(25, 90)
            ev = self._restamp(ev, now - shift * 60.0)
            self.counts[late] += 1
            return [Emitted(topic_key, formats.event_key(ev), json.dumps(ev), now - shift * 60.0, late)]
        if r < self.late_rate + self.anomaly_rate * 0.06:
            ev = self._restamp(ev, now + 2 * 86400)
            self.counts["future_timestamp"] += 1
            return [Emitted(topic_key, formats.event_key(ev), json.dumps(ev), now + 2 * 86400, "future_timestamp")]
        out = []
        for kind, payload in self.sim.maybe_defect(ev, self.anomaly_rate):
            self.counts[kind] += 1
            out.append(Emitted(topic_key, formats.event_key(ev), payload, now, kind))
        return out

    def events(self, duration_sec: float = 0.0, max_events: int = 0,
               sleep=time.sleep, clock=None, should_stop=lambda: False) -> Iterator[Emitted]:
        clock = clock or self.clock
        start = clock()
        emitted = 0
        while True:
            now = clock()
            if should_stop() or (duration_sec and now - start >= duration_sec):
                return
            self._advance(now)
            batch: list[Emitted] = []
            while self._reports and self._reports[0][0] <= now:
                tr, _, ev = heapq.heappop(self._reports)
                batch += self._finalize("incidents", self._restamp(ev, tr), now)
            while self._heap and self._heap[0][0] <= now:
                due, i, kind, src = heapq.heappop(self._heap)
                ev, topic_key = self._encode(kind, src, now)
                batch += self._finalize(topic_key, ev, now)
                heapq.heappush(self._heap, (due + self.interval * self.sim.rng.uniform(0.8, 1.2), i, kind, src))
            for e in batch:
                self.counts[f"emitted.{e.topic_key}"] += 1
                emitted += 1
                yield e
                if max_events and emitted >= max_events:
                    return
            next_due = min([self._heap[0][0]] if self._heap else [now + 1.0])
            if self._reports:
                next_due = min(next_due, self._reports[0][0])
            sleep(max(0.05, min(1.0, next_due - clock())))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> None:
    cfg = get_config()
    g = cfg["generator"]
    ap = argparse.ArgumentParser(description="AEGIS synthetic data generator (all output is synthetic)")
    sub = ap.add_subparsers(dest="mode", required=True)
    h = sub.add_parser("historical", help="write historical JSONL backfill")
    h.add_argument("--days", type=int, default=g["historical_days"])
    h.add_argument("--seed", type=int, default=g["seed"])
    h.add_argument("--step-minutes", type=int, default=g["historical_step_minutes"])
    h.add_argument("--end-date", help="local (IST) end date YYYY-MM-DD; default = today 00:00 IST")
    h.add_argument("--anomaly-rate", type=float, default=None)
    h.add_argument("--out", default=None, help="output dir (default data/generated/historical)")
    s = sub.add_parser("sample", help="small deterministic sample (2 days) for quick demos/tests")
    s.add_argument("--seed", type=int, default=g["seed"])
    s.add_argument("--out", default=None, help="output dir (default data/sample)")
    args = ap.parse_args(argv)

    if args.mode == "historical":
        end = None
        if args.end_date:
            end = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=formats.IST)
        out = Path(args.out) if args.out else data_path(cfg, "generated_dir", "historical")
        m = generate_historical(cfg, out, args.seed, args.days, args.step_minutes, end, args.anomaly_rate)
    else:
        out = Path(args.out) if args.out else (data_path(cfg, "generated_dir").parent / "sample")
        end = datetime(2026, 10, 15, tzinfo=formats.IST)   # fixed date -> byte-identical sample
        m = generate_historical(cfg, out, args.seed, 2, 30, end, 0.05, reference_dir=out / "reference")
    print(json.dumps({"out": str(out), "line_counts": m["line_counts"],
                      "injected_anomalies": m["injected_anomalies"],
                      "flood_severity_label_counts": m["flood_severity_label_counts"]}, indent=2))


if __name__ == "__main__":
    main()
