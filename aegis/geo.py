"""Geospatial primitives: coordinate validation, Haversine distance, the
analysis grid and nearest-station assignment.

Zones are cells of a regular latitude/longitude grid laid over the study
region bounding box. A zone id has the form ``Z-R<row>-C<col>`` where row 0 is
the southern edge and column 0 the western edge. Points on the northern or
eastern boundary belong to the last row/column. Points outside the bounding
box have no zone.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

import numpy as np

EARTH_RADIUS_KM = 6371.0088  # mean Earth radius (IUGG)


def is_valid_coordinate(lat: object, lon: object) -> bool:
    """True when both values are finite numbers within WGS84 ranges."""
    try:
        la, lo = float(lat), float(lon)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    if math.isnan(la) or math.isnan(lo) or math.isinf(la) or math.isinf(lo):
        return False
    return -90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres between two WGS84 points."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(min(1.0, math.sqrt(a)))


def haversine_km_vec(lat: float, lon: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Vectorised distance (km) from one point to arrays of points."""
    p1 = np.radians(lat)
    p2 = np.radians(np.asarray(lats, dtype=float))
    dp = p2 - p1
    dl = np.radians(np.asarray(lons, dtype=float) - lon)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.minimum(1.0, np.sqrt(a)))


@dataclass(frozen=True)
class Zone:
    zone_id: str
    row: int
    col: int
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    @property
    def center(self) -> tuple[float, float]:
        return ((self.min_lat + self.max_lat) / 2.0, (self.min_lon + self.max_lon) / 2.0)


class ZoneGrid:
    """Regular grid of analysis zones over a bounding box."""

    def __init__(self, min_lat: float, max_lat: float, min_lon: float, max_lon: float, cell_deg: float):
        if cell_deg <= 0:
            raise ValueError("cell_deg must be positive")
        self.min_lat, self.max_lat = float(min_lat), float(max_lat)
        self.min_lon, self.max_lon = float(min_lon), float(max_lon)
        self.cell = float(cell_deg)
        self.rows = max(1, int(round((self.max_lat - self.min_lat) / self.cell)))
        self.cols = max(1, int(round((self.max_lon - self.min_lon) / self.cell)))

    @classmethod
    def from_config(cls, cfg: dict) -> "ZoneGrid":
        r = cfg["region"]
        return cls(r["min_lat"], r["max_lat"], r["min_lon"], r["max_lon"], r["grid_cell_deg"])

    @staticmethod
    def zone_id_for(row: int, col: int) -> str:
        return f"Z-R{row:02d}-C{col:02d}"

    def contains(self, lat: float, lon: float) -> bool:
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon

    def cell_of(self, lat: float, lon: float) -> Optional[tuple[int, int]]:
        if not is_valid_coordinate(lat, lon) or not self.contains(float(lat), float(lon)):
            return None
        # floor(a / b), not a // b: identical to the Spark expression in
        # spark_jobs/transforms.py (the two can differ for some floats).
        row = min(int(math.floor((float(lat) - self.min_lat) / self.cell)), self.rows - 1)
        col = min(int(math.floor((float(lon) - self.min_lon) / self.cell)), self.cols - 1)
        return row, col

    def zone_for(self, lat: object, lon: object) -> Optional[str]:
        cell = self.cell_of(lat, lon)  # type: ignore[arg-type]
        return None if cell is None else self.zone_id_for(*cell)

    def zone(self, row: int, col: int) -> Zone:
        return Zone(
            zone_id=self.zone_id_for(row, col), row=row, col=col,
            min_lat=round(self.min_lat + row * self.cell, 6),
            min_lon=round(self.min_lon + col * self.cell, 6),
            max_lat=round(min(self.min_lat + (row + 1) * self.cell, self.max_lat), 6),
            max_lon=round(min(self.min_lon + (col + 1) * self.cell, self.max_lon), 6),
        )

    def zones(self) -> list[Zone]:
        return [self.zone(r, c) for r in range(self.rows) for c in range(self.cols)]

    def get(self, zone_id: str) -> Optional[Zone]:
        try:
            _, r, c = zone_id.split("-")
            row, col = int(r[1:]), int(c[1:])
        except (ValueError, AttributeError):
            return None
        if 0 <= row < self.rows and 0 <= col < self.cols:
            return self.zone(row, col)
        return None


def nearest_station_map(zones: Iterable[Zone], stations: Sequence[dict], radius_km: float) -> dict[str, tuple[str, float]]:
    """Map each zone id to its nearest station ``(station_id, distance_km)``
    when the station lies within ``radius_km`` of the zone centre."""
    if not stations:
        return {}
    lats = np.array([s["lat"] for s in stations], dtype=float)
    lons = np.array([s["lon"] for s in stations], dtype=float)
    out: dict[str, tuple[str, float]] = {}
    for z in zones:
        clat, clon = z.center
        d = haversine_km_vec(clat, clon, lats, lons)
        i = int(np.argmin(d))
        if d[i] <= radius_km:
            out[z.zone_id] = (stations[i]["station_id"], round(float(d[i]), 3))
    return out


def approx_coast_lon(lat: float) -> float:
    """Very rough synthetic land mask for the demo region: longitude of the
    coastline at a given latitude. Used only by the data generator to keep
    synthetic sensors and incidents on land."""
    return 80.25 + (lat - 12.85) * 0.2


def is_on_land(lat: float, lon: float, margin_deg: float = 0.006) -> bool:
    return lon < approx_coast_lon(lat) - margin_deg
