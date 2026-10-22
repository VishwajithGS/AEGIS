"""Observed daily rainfall: IMD 0.25° gridded data and NOAA GHCN-Daily.

IMD (India Meteorological Department, Pune) gridded rainfall — official,
interpolated from IMD rain gauges (Pai et al. 2014). Verified 2026-10-22:
``POST https://www.imdpune.gov.in/cmpg/Griddata/RF25.php`` with ``RF25=<year>``
returns a 25 MB classic NetCDF-3 file (``RAINFALL`` mm/day, −999 = missing /
sea). Coastal cells (e.g. 13.0 °N 80.25 °E) are masked; each zone therefore
takes the nearest *valid land* cell and records the distance.

NOAA GHCN-Daily station IN020040900 (MADRAS/MINAMBAKKAM, WMO 43279): ``PRCP``
in tenths of mm. Verified: 2023-12-04 = 249.9 mm.

**Day convention — not confirmed.** IMD daily totals are 24 h accumulations
read at 08:30 IST (03:00 UTC), but whether "date D" means the 24 h *starting*
or *ending* at 08:30 IST on D could not be confirmed from IMD documentation.
A test against ERA5 over all 2 400 zone-days (2023-11/12) is inconclusive
(r = 0.745 starting vs 0.732 ending; the NOAA gauge favours "ending",
r = 0.79 vs 0.71). AEGIS *assumes* "starting" for ``window_start`` /
``window_end`` and records ``window_convention`` on every record; the
validation report shows ERA5 agreement under both alignments.

IMD intensity classes (IMD terminology for 24 h rainfall) are attached as a
*derived* category: no rain < 2.5, light 2.5–15.5, moderate 15.6–64.4,
heavy 64.5–115.5, very heavy 115.6–204.4, extremely heavy ≥ 204.5 mm.
"""
from __future__ import annotations

import csv
import io
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from aegis.geo import ZoneGrid, haversine_km
from aegis.realdata.common import Provenance, RealDataError, fetch, stable_id, utc_iso
from aegis.realdata.netcdf3 import open_dataset

IMD_URL = "https://www.imdpune.gov.in/cmpg/Griddata/RF25.php"
IMD_PROVENANCE = Provenance(
    data_source="imd-gridded-0.25",
    observation_type="observation_gridded",
    source_url="https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_NetCDF.html",
    licence="IMD Pune free download; no licence text on the page; cite Pai et al. (2014), Mausam 65(1):1-18",
    citation="Pai D.S. et al. (2014) Development of a new high spatial resolution (0.25x0.25) long period daily gridded rainfall data set over India",
)
GHCND_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{station}.csv"
GHCND_PROVENANCE = Provenance(
    data_source="noaa-ghcnd",
    observation_type="observation",
    source_url="https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily",
    licence="NOAA NCEI; no use constraints stated on the product page; cite Menne et al. (2012)",
    citation="Menne M.J. et al. (2012) An overview of the GHCN-Daily database, J. Atmos. Oceanic Technol.",
)
GHCND_STATIONS = {"IN020040900": "MADRAS/MINAMBAKKAM (WMO 43279)"}
SCHEMA_VERSION = "rf-daily-1"
DAY_START_UTC_HOUR = 3            # 08:30 IST = 03:00 UTC
WINDOW_CONVENTION = ("assumed: 24 h starting 08:30 IST (03:00 UTC) on the date; not confirmed by IMD documentation "
                     "- see real_data_validation.json for both alignments")

IMD_CATEGORIES = [(2.5, "no_rain"), (15.6, "light"), (64.5, "moderate"), (115.6, "heavy"),
                  (204.5, "very_heavy"), (math.inf, "extremely_heavy")]


def imd_category(mm: Optional[float]) -> Optional[str]:
    if mm is None or (isinstance(mm, float) and math.isnan(mm)):
        return None
    if mm < 0:
        raise RealDataError(f"negative rainfall {mm}")
    for upper, name in IMD_CATEGORIES:
        if mm < upper:
            return name
    return "extremely_heavy"


def day_window(d: date) -> tuple[str, str]:
    start = datetime(d.year, d.month, d.day, DAY_START_UTC_HOUR, tzinfo=timezone.utc)
    return utc_iso(start), utc_iso(start + timedelta(days=1))


def _record(source_id: str, d: date, mm: Optional[float], zone_id: Optional[str], lat: float, lon: float,
            prov: Provenance, retrieved_at: str, **extra) -> dict:
    ws, we = day_window(d)
    return {"event_id": stable_id("RFD", prov.data_source, source_id, zone_id or "-", d.isoformat()),
            "record_type": "daily_rainfall", "schema_version": SCHEMA_VERSION, "synthetic": False,
            "source_id": source_id, "zone_id": zone_id, "date": d.isoformat(),
            "window_start": ws, "window_end": we, "window_convention": WINDOW_CONVENTION, "lat": lat, "lon": lon,
            "rainfall_mm_day": None if mm is None else round(float(mm), 2), "imd_category": imd_category(mm),
            "retrieved_at": retrieved_at, **prov.as_fields(), **extra}


# --------------------------------------------------------------------------- IMD
def imd_year(year: int, cache_dir: Path, refresh: bool = False) -> tuple[Path, dict]:
    raw, res = fetch(IMD_URL, cache_dir, data={"RF25": str(year)}, refresh=refresh, suffix=".nc", timeout=900)
    if raw[:3] != b"CDF":
        raise RealDataError(f"IMD response for {year} is not a NetCDF file (got {raw[:16]!r})")
    return Path(res.path), res.manifest()


def imd_records(nc_path: Path, grid: ZoneGrid, start: date, end: date, retrieved_at: str) -> list[dict]:
    ds = open_dataset(nc_path)
    for v in ("RAINFALL", "LATITUDE", "LONGITUDE", "TIME"):
        if v not in ds.variables:
            raise RealDataError(f"IMD file lacks variable {v}")
    if ds.variables["RAINFALL"].attrs.get("units") != "mm":
        raise RealDataError("IMD RAINFALL units are not mm")
    lat, lon, t = ds.read("LATITUDE"), ds.read("LONGITUDE"), ds.read("TIME")
    rf = ds.read("RAINFALL")
    origin = date(1900, 12, 31)
    days = [origin + timedelta(days=int(x)) for x in t]
    valid_any = np.isfinite(rf).any(axis=0)                 # land cells
    land = np.argwhere(valid_any)
    out = []
    for z in grid.zones():
        zl, zo = z.center
        dists = [haversine_km(zl, zo, float(lat[i]), float(lon[j])) for i, j in land]
        k = int(np.argmin(dists))
        i, j = land[k]
        sid = f"IMD-{float(lat[i]):.2f}N-{float(lon[j]):.2f}E"
        for di, d in enumerate(days):
            if start <= d <= end:
                v = rf[di, i, j]
                out.append(_record(sid, d, None if np.isnan(v) else float(v), z.zone_id, round(zl, 4), round(zo, 4),
                                   IMD_PROVENANCE, retrieved_at, grid_lat=float(lat[i]), grid_lon=float(lon[j]),
                                   cell_distance_km=round(float(dists[k]), 2)))
    return out


# --------------------------------------------------------------------------- GHCN-D
def ghcnd_station(station: str, cache_dir: Path, refresh: bool = False) -> tuple[bytes, dict]:
    raw, res = fetch(GHCND_URL.format(station=station), cache_dir, refresh=refresh, suffix=".csv", timeout=300)
    return raw, res.manifest()


def ghcnd_records(raw: bytes, grid: ZoneGrid, start: date, end: date, retrieved_at: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    need = {"STATION", "DATE", "LATITUDE", "LONGITUDE", "NAME", "PRCP"}
    if not reader.fieldnames or not need <= set(reader.fieldnames):
        raise RealDataError(f"GHCN-D CSV lacks columns {sorted(need - set(reader.fieldnames or []))}")
    out = []
    for row in reader:
        d = date.fromisoformat(row["DATE"])
        if not (start <= d <= end):
            continue
        lat, lon = float(row["LATITUDE"]), float(row["LONGITUDE"])
        p = row["PRCP"].strip()
        mm = None if p == "" else int(p) / 10.0            # tenths of mm -> mm
        if mm is not None and mm < 0:
            raise RealDataError(f"negative PRCP on {row['DATE']}")
        attrs = (row.get("PRCP_ATTRIBUTES") or "").split(",")
        out.append(_record(f"GHCND-{row['STATION']}", d, mm, grid.zone_for(lat, lon), lat, lon, GHCND_PROVENANCE,
                           retrieved_at, station_name=row["NAME"].strip(),
                           quality_flag=(attrs[1] if len(attrs) > 1 else "") or None,
                           source_flag=(attrs[2] if len(attrs) > 2 else "") or None))
    return out
