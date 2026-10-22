"""Real-data ingestion layer: parsing, validation, unit/time/coordinate conversion,
provenance, idempotency. Offline: uses real response excerpts in tests/fixtures/real."""
from __future__ import annotations

import http.server
import json
import struct
import threading
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from aegis.geo import ZoneGrid
from aegis.realdata import daily_rainfall, era5, events
from aegis.realdata.common import RealDataError, fetch, stable_id, write_jsonl
from aegis.realdata.netcdf3 import NetCDF3Error, open_dataset

FIX = Path(__file__).resolve().parent.parent / "fixtures" / "real"


# --------------------------------------------------------------------------- NetCDF-3 reader
def _name(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">i", len(b)) + b + b"\0" * ((4 - len(b) % 4) % 4)


def _write_tiny_cdf(path: Path, rain: np.ndarray, lats, lons, times) -> None:
    """Minimal classic NetCDF-3 writer (IMD layout: TIME record dim, RAINFALL(TIME, LAT, LON))."""
    nlat, nlon, nt = len(lats), len(lons), len(times)

    def att_list(atts):
        if not atts:
            return struct.pack(">ii", 0, 0)
        out = struct.pack(">ii", 0x0C, len(atts))
        for k, (typ, val) in atts.items():
            out += _name(k)
            if typ == 2:
                b = val.encode()
                out += struct.pack(">ii", 2, len(b)) + b + b"\0" * ((4 - len(b) % 4) % 4)
            else:
                out += struct.pack(">ii", 5, 1) + struct.pack(">f", val)
        return out

    dims = struct.pack(">ii", 0x0A, 3) + _name("LONGITUDE") + struct.pack(">i", nlon) + _name("LATITUDE") + \
        struct.pack(">i", nlat) + _name("TIME") + struct.pack(">i", 0)
    specs = [("LONGITUDE", [0], {"units": (2, "degrees_east")}, 6, 8 * nlon, False),
             ("LATITUDE", [1], {"units": (2, "degrees_north")}, 6, 8 * nlat, False),
             ("TIME", [2], {"units": (2, "days since 1900-12-31 00:00:00")}, 6, 8, True),
             ("RAINFALL", [2, 1, 0], {"units": (2, "mm"), "missing_value": (5, -999.0), "_FillValue": (5, -999.0)},
              5, 4 * nlat * nlon, True)]

    def var_list(begins):
        out = struct.pack(">ii", 0x0B, len(specs))
        for (name, dimids, atts, typ, vsize, _), begin in zip(specs, begins):
            out += _name(name) + struct.pack(">i", len(dimids)) + struct.pack(f">{len(dimids)}i", *dimids)
            out += att_list(atts) + struct.pack(">iii", typ, vsize, begin)
        return out

    header_len = len(b"CDF\x01" + struct.pack(">i", nt) + dims + att_list({}) + var_list([0, 0, 0, 0]))
    b_lon = header_len
    b_lat = b_lon + 8 * nlon
    b_rec = b_lat + 8 * nlat
    begins = [b_lon, b_lat, b_rec, b_rec + 8]
    body = b"CDF\x01" + struct.pack(">i", nt) + dims + att_list({}) + var_list(begins)
    body += np.asarray(lons, ">f8").tobytes() + np.asarray(lats, ">f8").tobytes()
    for k in range(nt):
        body += np.asarray([times[k]], ">f8").tobytes() + np.asarray(rain[k], ">f4").tobytes()
    path.write_bytes(body)


def test_netcdf3_reader_roundtrip(tmp_path):
    rain = np.array([[[1.5, -999.0], [244.0, 0.0]], [[2.0, 3.0], [-999.0, 70.25]]])
    p = tmp_path / "t.nc"
    _write_tiny_cdf(p, rain, lats=[13.0, 13.25], lons=[80.0, 80.25], times=[44898.0, 44899.0])
    ds = open_dataset(p)
    assert ds.dims == {"LONGITUDE": 2, "LATITUDE": 2, "TIME": 0} and ds.numrecs == 2
    r = ds.read("RAINFALL")
    assert r.shape == (2, 2, 2) and r[0, 1, 0] == 244.0 and r[1, 1, 1] == 70.25
    assert np.isnan(r[0, 0, 1]) and np.isnan(r[1, 1, 0])            # -999 -> NaN
    assert list(ds.read("TIME")) == [44898.0, 44899.0]
    assert ds.variables["RAINFALL"].attrs["units"] == "mm"


def test_imd_records_nearest_land_cell_and_dates(tmp_path, cfg):
    # 13.0N/80.25E is "sea" (all NaN) -> zones must fall back to a valid land cell
    rain = np.array([[[244.0, -999.0], [5.0, -999.0]]])
    p = tmp_path / "imd.nc"
    _write_tiny_cdf(p, rain, lats=[13.0, 13.25], lons=[80.0, 80.25], times=[44898.0])   # 1900-12-31 + 44898 = 2023-12-04
    recs = daily_rainfall.imd_records(p, ZoneGrid.from_config(cfg), date(2023, 12, 4), date(2023, 12, 4), "t")
    assert len(recs) == 40 and {r["date"] for r in recs} == {"2023-12-04"}
    assert all(r["grid_lon"] == 80.0 for r in recs)                  # the masked column is never used
    z = next(r for r in recs if r["zone_id"] == "Z-R03-C01")
    assert z["rainfall_mm_day"] == 244.0 and z["imd_category"] == "extremely_heavy"
    assert z["window_start"] == "2023-12-04T03:00:00Z" and z["window_end"] == "2023-12-05T03:00:00Z"
    assert z["synthetic"] is False and z["observation_type"] == "observation_gridded"


def test_netcdf3_rejects_other_formats(tmp_path):
    p = tmp_path / "x.nc"
    p.write_bytes(b"\x89HDF\r\n\x1a\n" + b"\0" * 20)
    with pytest.raises(NetCDF3Error):
        open_dataset(p)


# --------------------------------------------------------------------------- ERA5
def test_era5_parse_real_response():
    obj = json.loads((FIX / "open_meteo_era5_13.0N_80.25E_2023-12-03_04.json").read_text(encoding="utf-8"))
    recs = era5.parse_location(obj, "Z-R03-C03", 13.025, 80.275, "2026-10-22T03:40:47Z")
    assert len(recs) == 48
    r0 = recs[0]
    assert r0["schema_version"] == "wx-era5-1" and r0["synthetic"] is False
    assert r0["observation_type"] == "reanalysis" and r0["data_source"] == "era5-open-meteo"
    assert r0["event_time"] == "2023-12-03T00:00:00Z"                # UTC, explicit Z
    assert (r0["grid_lat"], r0["grid_lon"]) == (13.0, 80.25)
    assert (r0["lat"], r0["lon"]) == (13.025, 80.275)                  # zone centre, so the zone is exact
    assert round(sum(r["rainfall_mm_hr"] for r in recs), 1) == 272.9  # matches the verified probe
    assert len({r["event_id"] for r in recs}) == 48


def test_era5_rejects_wrong_units_and_timezone():
    obj = json.loads((FIX / "open_meteo_era5_13.0N_80.25E_2023-12-03_04.json").read_text(encoding="utf-8"))
    bad = json.loads(json.dumps(obj))
    bad["hourly_units"]["precipitation"] = "inch"
    with pytest.raises(RealDataError, match="unit"):
        era5.parse_location(bad, "Z", 13.0, 80.25, "t")
    bad = json.loads(json.dumps(obj))
    bad["timezone"] = "Asia/Kolkata"
    with pytest.raises(RealDataError, match="time zone"):
        era5.parse_location(bad, "Z", 13.0, 80.25, "t")
    bad = json.loads(json.dumps(obj))
    del bad["hourly"]["precipitation"]
    with pytest.raises(RealDataError, match="missing"):
        era5.parse_location(bad, "Z", 13.0, 80.25, "t")


def test_era5_fixture_records_are_consistent():
    recs = [json.loads(l) for l in (FIX / "era5_weather_2zones_2023-12-02_05.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(recs) == 192 and all(r["synthetic"] is False for r in recs)
    grid = None
    for r in recs:
        assert r["event_time"].endswith("Z") and r["rainfall_mm_hr"] >= 0
    stations = era5.virtual_stations(recs)
    assert [s["station_id"] for s in stations] == ["ERA5-Z-R03-C01", "ERA5-Z-R03-C02"]


# --------------------------------------------------------------------------- daily rainfall
@pytest.mark.parametrize("mm,cat", [(0.0, "no_rain"), (2.4, "no_rain"), (2.5, "light"), (15.5, "light"),
                                    (15.6, "moderate"), (64.4, "moderate"), (64.5, "heavy"), (115.6, "very_heavy"),
                                    (204.4, "very_heavy"), (204.5, "extremely_heavy"), (None, None)])
def test_imd_categories(mm, cat):
    assert daily_rainfall.imd_category(mm) == cat


def test_imd_category_rejects_negative():
    with pytest.raises(RealDataError):
        daily_rainfall.imd_category(-1.0)


def test_ghcnd_parse_real_csv(cfg):
    raw = (FIX / "ghcnd_IN020040900_2023-11-28_12-08.csv").read_bytes()
    recs = daily_rainfall.ghcnd_records(raw, ZoneGrid.from_config(cfg), date(2023, 11, 28), date(2023, 12, 8), "t")
    by_day = {r["date"]: r for r in recs}
    assert by_day["2023-12-04"]["rainfall_mm_day"] == 249.9            # 2499 tenths of mm
    assert by_day["2023-12-04"]["imd_category"] == "extremely_heavy"
    assert by_day["2023-12-05"]["rainfall_mm_day"] == 188.0
    assert by_day["2023-12-06"]["rainfall_mm_day"] == 0.0
    assert all(r["zone_id"] == "Z-R03-C01" for r in recs)              # 13.0N 80.183E
    assert by_day["2023-12-04"]["source_flag"] == "S" and by_day["2023-12-04"]["observation_type"] == "observation"
    missing = [r for r in recs if r["rainfall_mm_day"] is None]       # days without PRCP stay missing
    assert all(r["imd_category"] is None for r in missing)


def test_ghcnd_rejects_unexpected_csv(cfg):
    with pytest.raises(RealDataError, match="lacks columns"):
        daily_rainfall.ghcnd_records(b"A,B\n1,2\n", ZoneGrid.from_config(cfg), date(2023, 1, 1), date(2023, 12, 31), "t")


# --------------------------------------------------------------------------- events
def test_parse_dfo_real_response():
    evs = events.parse_dfo(json.loads((FIX / "dfo_query_study_box.json").read_text(encoding="utf-8")), "t")
    assert len(evs) == 12                                   # events intersecting the exact study box
    nilam = next(e for e in evs if e["source_event_id"] == "DFO-4001")
    assert nilam["began"] == "2012-11-04T00:00:00Z" and nilam["severity_label"] == "extreme" and nilam["dead"] == 40
    # the 2015 Chennai flood polygon does NOT intersect the exact box (coarse DFO polygons): documented limitation
    assert "DFO-4309" not in {e["source_event_id"] for e in evs}
    assert all(e["record_type"] == "official_event" and e["synthetic"] is False for e in evs)
    with pytest.raises(RealDataError):
        events.parse_dfo({"error": {"code": 400}}, "t")


def test_parse_gdacs_real_response():
    evs = events.parse_gdacs(json.loads((FIX / "gdacs_india_2023-11-01_12-30.json").read_text(encoding="utf-8")), "t")
    m = next(e for e in evs if "MICHAUNG" in e["name"])
    assert m["alert_level"] == "Orange" and m["event_kind"] == "tropical_cyclone"
    assert m["began"] == "2023-12-03T00:00:00Z" and m["severity_label"] is None   # no invented DFO class


# --------------------------------------------------------------------------- common
def test_stable_ids_and_deterministic_jsonl(tmp_path):
    assert stable_id("ERA5", "Z-R00-C00", "2023-12-04T00:00") == stable_id("ERA5", "Z-R00-C00", "2023-12-04T00:00")
    assert stable_id("ERA5", "Z-R00-C00", "t1") != stable_id("ERA5", "Z-R00-C01", "t1")
    recs = [{"b": 2, "a": 1}, {"a": 3, "b": None}]
    write_jsonl(tmp_path / "a.jsonl", recs)
    write_jsonl(tmp_path / "b.jsonl", recs)
    assert (tmp_path / "a.jsonl").read_bytes() == (tmp_path / "b.jsonl").read_bytes()


class _Handler(http.server.BaseHTTPRequestHandler):
    hits = 0

    def do_GET(self):  # noqa: N802
        _Handler.hits += 1
        if self.path.startswith("/missing"):
            self.send_response(404); self.end_headers(); return
        body = b'{"ok": true}'
        self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def test_fetch_caches_raw_response_and_fails_loudly(tmp_path):
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        raw1, r1 = fetch(base + "/data", tmp_path, params={"x": "1"}, suffix=".json", retries=1)
        raw2, r2 = fetch(base + "/data", tmp_path, params={"x": "1"}, suffix=".json", retries=1)
        assert raw1 == raw2 == b'{"ok": true}' and not r1.cached and r2.cached and r1.sha256 == r2.sha256
        assert _Handler.hits == 1                                         # second call served from the cache
        assert Path(r1.path).read_bytes() == raw1                         # raw file not overwritten by metadata
        with pytest.raises(RealDataError):
            fetch(base + "/missing", tmp_path, retries=3, backoff_s=0)
    finally:
        srv.shutdown()


def test_ingest_rejects_future_end_date():
    from aegis.realdata.ingest import main
    with pytest.raises(SystemExit):
        main(["--end", "2999-01-01"])
