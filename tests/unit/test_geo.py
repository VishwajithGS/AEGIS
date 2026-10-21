import math

import numpy as np
import pytest

from aegis.geo import ZoneGrid, haversine_km, haversine_km_vec, is_valid_coordinate, nearest_station_map


@pytest.fixture()
def grid(cfg):
    return ZoneGrid.from_config(cfg)


@pytest.mark.parametrize("lat,lon,ok", [
    (13.05, 80.2, True), (-90, -180, True), (90, 180, True), (90.0001, 0, False), (0, 180.5, False),
    (None, 80.2, False), ("abc", 80.2, False), (float("nan"), 80.2, False), (float("inf"), 0, False), ("13.1", "80.2", True),
])
def test_coordinate_validation(lat, lon, ok):
    assert is_valid_coordinate(lat, lon) is ok


def test_haversine_one_degree_on_equator():
    # One degree of longitude on the equator = 2*pi*R/360 with R = 6371.0088 km
    assert haversine_km(0, 0, 0, 1) == pytest.approx(2 * math.pi * 6371.0088 / 360, rel=1e-9)


def test_haversine_properties():
    a, b = (13.0827, 80.2707), (12.9941, 80.1709)
    assert haversine_km(*a, *a) == 0
    assert haversine_km(*a, *b) == pytest.approx(haversine_km(*b, *a))
    assert 13 < haversine_km(*a, *b) < 16          # ~14.5 km
    vec = haversine_km_vec(a[0], a[1], np.array([b[0], a[0]]), np.array([b[1], a[1]]))
    assert vec[0] == pytest.approx(haversine_km(*a, *b)) and vec[1] == pytest.approx(0)


def test_grid_shape_and_ids(grid):
    zones = grid.zones()
    assert (grid.rows, grid.cols) == (8, 5)
    assert len(zones) == 40 and len({z.zone_id for z in zones}) == 40
    assert zones[0].zone_id == "Z-R00-C00" and zones[-1].zone_id == "Z-R07-C04"
    for z in zones:
        assert grid.get(z.zone_id) == z
        assert grid.zone_for(*z.center) == z.zone_id


def test_zone_assignment_edges(grid):
    assert grid.zone_for(12.85, 80.10) == "Z-R00-C00"          # south-west corner
    assert grid.zone_for(13.25, 80.35) == "Z-R07-C04"          # north-east corner -> last cell (clamped)
    assert grid.zone_for(12.8999, 80.1499) == "Z-R00-C00"
    assert grid.zone_for(12.90, 80.15) == "Z-R01-C01"          # cell boundary belongs to the next cell
    assert grid.zone_for(13.30, 80.20) is None                 # outside the study region
    assert grid.zone_for(95, 80.2) is None                     # invalid coordinate
    assert grid.zone_for(None, None) is None
    assert grid.get("Z-R99-C00") is None and grid.get("garbage") is None


def test_nearest_station_map_respects_radius(grid):
    stations = [{"station_id": "S1", "lat": 12.875, "lon": 80.125}, {"station_id": "S2", "lat": 13.225, "lon": 80.325}]
    m = nearest_station_map(grid.zones(), stations, radius_km=3.0)
    assert m["Z-R00-C00"][0] == "S1" and m["Z-R00-C00"][1] < 0.01
    assert m["Z-R07-C04"][0] == "S2"
    assert "Z-R04-C02" not in m                                # too far from both stations
    assert nearest_station_map(grid.zones(), [], 5.0) == {}
