"""ERA5 hourly reanalysis via the Open-Meteo Historical Weather API.

Verified 2026-10-22: ``archive-api.open-meteo.com/v1/archive`` (no key) returns
hourly ``precipitation`` (mm, sum of the preceding hour), ``temperature_2m``
(°C) and ``relative_humidity_2m`` (%) for ERA5 0.25° cells. ``models=era5`` is
requested explicitly: ``era5_land`` returned 0 mm at Chennai during Cyclone
Michaung and is not used for rainfall.

One "virtual station" per AEGIS zone: the zone centre is queried and the API
snaps it to its ERA5 cell, recorded as ``grid_lat``/``grid_lon``. Several zones
can share one cell (identical values) — that is the real resolution of ERA5.

Output records use schema version ``wx-era5-1`` on the ``weather-events``
family; they map onto the canonical weather columns in
``spark_jobs/transforms.py``. ERA5 is a **reanalysis** (model + assimilated
observations), not a rain-gauge measurement; every record says so.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from aegis.geo import ZoneGrid
from aegis.realdata.common import Provenance, RealDataError, fetch, require_fields, stable_id, utc_iso

API = "https://archive-api.open-meteo.com/v1/archive"
PROVENANCE = Provenance(
    data_source="era5-open-meteo",
    observation_type="reanalysis",
    source_url="https://open-meteo.com/en/docs/historical-weather-api",
    licence="CC BY 4.0 (Open-Meteo); free API non-commercial, <10,000 calls/day; contains modified Copernicus ERA5 data",
    citation="Hersbach et al. (2020) ERA5; Weather data by Open-Meteo.com",
)
SCHEMA_VERSION = "wx-era5-1"
EXPECTED_UNITS = {"precipitation": "mm", "temperature_2m": "°C", "relative_humidity_2m": "%"}
HOURLY_VARS = ("precipitation", "temperature_2m", "relative_humidity_2m")


def zone_points(grid: ZoneGrid) -> list[tuple[str, float, float]]:
    return [(z.zone_id, round(z.center[0], 4), round(z.center[1], 4)) for z in grid.zones()]


def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
        return float(v)
    raise RealDataError(f"non-numeric value {v!r} in ERA5 response")


def parse_location(obj: dict, zone_id: str, zone_lat: float, zone_lon: float, retrieved_at: str) -> list[dict]:
    """Validate one Open-Meteo location object and convert it to ``wx-era5-1`` records."""
    require_fields(obj, ("latitude", "longitude", "hourly", "hourly_units"), "ERA5 location")
    hourly, units = obj["hourly"], obj["hourly_units"]
    require_fields(hourly, ("time",) + HOURLY_VARS, "ERA5 hourly block")
    for var, unit in EXPECTED_UNITS.items():
        if units.get(var) != unit:
            raise RealDataError(f"ERA5 unit for {var} is {units.get(var)!r}, expected {unit!r}")
    n = len(hourly["time"])
    if any(len(hourly[v]) != n for v in HOURLY_VARS):
        raise RealDataError("ERA5 hourly arrays have different lengths")
    tz = obj.get("timezone", "GMT")
    if tz not in ("GMT", "UTC"):
        raise RealDataError(f"ERA5 response time zone is {tz!r}; request timezone=UTC")
    grid_lat, grid_lon = round(float(obj["latitude"]), 4), round(float(obj["longitude"]), 4)
    out = []
    for i, t in enumerate(hourly["time"]):
        ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)   # end of the accumulation hour
        out.append({
            "event_id": stable_id("ERA5", zone_id, t),
            "source_type": "weather",
            "source_id": f"ERA5-{zone_id}",
            "schema_version": SCHEMA_VERSION,
            "synthetic": False,
            "event_time": utc_iso(ts),
            "lat": zone_lat, "lon": zone_lon,
            "zone_hint": zone_id,
            "grid_lat": grid_lat, "grid_lon": grid_lon,
            "grid_elevation_m": obj.get("elevation"),
            "rainfall_mm_hr": _num(hourly["precipitation"][i]),
            "temperature_c": _num(hourly["temperature_2m"][i]),
            "humidity_pct": _num(hourly["relative_humidity_2m"][i]),
            "accumulation": "sum over the hour ending at event_time",
            "retrieved_at": retrieved_at,
            **PROVENANCE.as_fields(),
        })
    return out


def download(grid: ZoneGrid, start: date, end: date, cache_dir: Path, refresh: bool = False) -> tuple[list[dict], list[dict]]:
    """All zones, hourly, ``start``..``end`` inclusive. Returns (records, fetch manifests)."""
    if end < start:
        raise RealDataError("end date before start date")
    pts = zone_points(grid)
    params = {
        "latitude": ",".join(str(p[1]) for p in pts),
        "longitude": ",".join(str(p[2]) for p in pts),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS), "models": "era5", "timezone": "UTC",
    }
    raw, res = fetch(API, cache_dir, params=params, refresh=refresh, suffix=".json")
    try:
        body = json.loads(raw)
    except ValueError as exc:
        raise RealDataError(f"ERA5 response is not JSON: {exc}") from exc
    if isinstance(body, dict) and body.get("error"):
        raise RealDataError(f"Open-Meteo error: {body.get('reason')}")
    locations = body if isinstance(body, list) else [body]
    if len(locations) != len(pts):
        raise RealDataError(f"expected {len(pts)} locations, got {len(locations)}")
    records: list[dict] = []
    for (zone_id, lat, lon), obj in zip(pts, locations):
        records += parse_location(obj, zone_id, lat, lon, res.retrieved_at)
    return records, [res.manifest()]


def virtual_stations(records: Iterable[dict]) -> list[dict]:
    """Station registry entries (one per zone) for the spatial assignment."""
    seen = {}
    for r in records:
        if r["source_id"] not in seen:
            seen[r["source_id"]] = {"station_id": r["source_id"], "name": f"ERA5 cell {r['grid_lat']}N {r['grid_lon']}E",
                                    "format": SCHEMA_VERSION, "lat": r["lat"], "lon": r["lon"],
                                    "grid_lat": r["grid_lat"], "grid_lon": r["grid_lon"],
                                    "data_source": r["data_source"], "observation_type": r["observation_type"]}
    return sorted(seen.values(), key=lambda s: s["station_id"])
