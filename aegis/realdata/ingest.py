"""Download and import verified real datasets into a separate data root.

    python -m aegis.realdata.ingest                                   # defaults below
    python -m aegis.realdata.ingest --start 2023-11-01 --end 2023-12-30 --sources era5,imd,ghcnd,dfo,gdacs
    python -m aegis.realdata.ingest --refresh                         # ignore the raw-response cache

Default period: 2023-11-01 – 2023-12-30 (60 days, Northeast monsoon incl.
Cyclone Michaung). Output (``--data-root``, default ``data_real``):

    raw/http_cache/                  raw API responses + SHA-256 (re-runs reuse them)
    raw/historical/weather.jsonl     ERA5 hourly per zone (wx-era5-1)  -> batch ETL input
    raw/historical/rainfall_daily.jsonl   IMD gridded + GHCN-D daily rainfall (rf-daily-1)
    raw/historical/official_events.jsonl  DFO + GDACS events (ev-official-1)
    raw/historical/manifest.json     sources, licences, fetches, counts (no injected defects)
    reference/stations.json, zones.json   ERA5 virtual stations, analysis grid
    dataset_manifest.json            data_mode = real-historical (read by the API / dashboard)

Never mixed with synthetic data: synthetic runs use ``data/``. Any download or
schema failure stops the import with an error (no fallback values).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from aegis.config import PROJECT_ROOT, get_config
from aegis.geo import ZoneGrid, haversine_km
from aegis.logging_config import setup_logging
from aegis.realdata import daily_rainfall, era5, events
from aegis.realdata.common import RealDataError, utc_iso, write_json, write_jsonl

log = setup_logging("realdata")
ALL_SOURCES = ("era5", "imd", "ghcnd", "dfo", "gdacs")
DEFAULT_START, DEFAULT_END = date(2023, 11, 1), date(2023, 12, 30)

NOT_AVAILABLE = {
    "water_level": "no verified open river-gauge water-level source (CWC/India-WRIS not accessible); GloFAS discharge is a different quantity",
    "traffic": "no verified open real-time traffic source for Chennai",
    "citizen_reports": "no verified open, licensed source of incident reports",
    "zone_level_incidents_and_severity_labels": "DFO/GDACS events are regional; no incident-level severity exists",
    "historical_hazard_index": "zone-level flood frequency needs the Global Flood Database (Google Earth Engine account)",
    "hourly_rain_gauge_observations": "NOAA ISD has no hourly precipitation for Chennai; hourly rain is ERA5 reanalysis",
}


SOURCE_DATA_IDS = {"era5": "era5-open-meteo", "imd": "imd-gridded-0.25", "ghcnd": "noaa-ghcnd",
                   "dfo": "dfo-flood-archive", "gdacs": "gdacs"}
FILE_SOURCES = {"weather.jsonl": ("era5",), "rainfall_daily.jsonl": ("imd", "ghcnd"),
                "official_events.jsonl": ("dfo", "gdacs")}


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _merge(path: Path, new: list[dict], rerun: list[str]) -> list[dict]:
    """Keep records of sources that were not re-run (partial imports), replace the others."""
    rerun_ids = {SOURCE_DATA_IDS[s] for s in rerun}
    return [r for r in _load_jsonl(path) if r.get("data_source") not in rerun_ids] + new


def _region_bbox(cfg: dict) -> tuple[float, float, float, float]:
    r = cfg["region"]
    return r["min_lon"], r["min_lat"], r["max_lon"], r["max_lat"]


def run(cfg: dict, data_root: Path, start: date, end: date, sources: list[str], refresh: bool = False) -> dict:
    grid = ZoneGrid.from_config(cfg)
    cache = data_root / "raw" / "http_cache"
    out_dir = data_root / "raw" / "historical"
    previous = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8")) if (out_dir / "manifest.json").exists() else None
    partial = set(sources) != set(ALL_SOURCES)
    if previous and partial and previous.get("period") != {"start": start.isoformat(), "end": end.isoformat()}:
        raise RealDataError(f"existing data_real covers {previous.get('period')}; a partial import for another "
                            f"period would mix periods. Re-import all sources (default --sources) instead.")
    fetches: dict[str, list] = {k: v for k, v in (previous or {}).get("fetches", {}).items() if k not in sources}
    counts: dict[str, int] = {}
    sections: dict[str, dict] = {k: v for k, v in (previous or {}).get("sources", {}).items() if k not in sources}

    if "era5" in sources:
        log.info(f"ERA5 hourly for {len(grid.zones())} zones, {start} .. {end}")
        recs, f = era5.download(grid, start, end, cache / "era5", refresh)
        fetches["era5"] = f
        counts["weather.jsonl"] = write_jsonl(out_dir / "weather.jsonl", sorted(_merge(out_dir / "weather.jsonl", recs, ["era5"]),
                                                                                   key=lambda r: (r["event_time"], r["source_id"])))
        missing = sum(1 for r in recs if r["rainfall_mm_hr"] is None)
        cells = sorted({(r["grid_lat"], r["grid_lon"]) for r in recs})
        sections["era5"] = {"records": len(recs), "zones": len({r["source_id"] for r in recs}), "era5_cells": cells,
                            "hours_missing_precipitation": missing, **era5.PROVENANCE.as_fields()}
        write_json(data_root / "reference" / "stations.json", {
            "synthetic": False, "note": "ERA5 virtual stations (one per zone centre); no real river gauges or traffic segments available",
            "weather_stations": era5.virtual_stations(recs), "water_stations": [], "traffic_segments": []})

    daily: list[dict] = []
    if "imd" in sources:
        for year in range(start.year, end.year + 1):
            log.info(f"IMD gridded rainfall {year}")
            nc, man = daily_rainfall.imd_year(year, cache / "imd", refresh)
            fetches.setdefault("imd", []).append(man)
            recs = daily_rainfall.imd_records(nc, grid, start, end, man["retrieved_at"])
            daily += recs
        imd = [r for r in daily if r["data_source"] == daily_rainfall.IMD_PROVENANCE.data_source]
        sections["imd"] = {"records": len(imd), "cells": sorted({r["source_id"] for r in imd}),
                           "max_cell_distance_km": max((r["cell_distance_km"] for r in imd), default=None),
                           "missing_days": sum(1 for r in imd if r["rainfall_mm_day"] is None),
                           **daily_rainfall.IMD_PROVENANCE.as_fields()}
    if "ghcnd" in sources:
        for st in daily_rainfall.GHCND_STATIONS:
            log.info(f"GHCN-Daily station {st}")
            raw, man = daily_rainfall.ghcnd_station(st, cache / "ghcnd", refresh)
            fetches.setdefault("ghcnd", []).append(man)
            recs = daily_rainfall.ghcnd_records(raw, grid, start, end, man["retrieved_at"])
            daily += recs
            sections.setdefault("ghcnd", {"stations": {}, **daily_rainfall.GHCND_PROVENANCE.as_fields()})["stations"][st] = {
                "records": len(recs), "missing_days": sum(1 for r in recs if r["rainfall_mm_day"] is None),
                "zone_id": recs[0]["zone_id"] if recs else None}
    rerun_daily = [s for s in ("imd", "ghcnd") if s in sources]
    if rerun_daily:
        daily = _merge(out_dir / "rainfall_daily.jsonl", daily, rerun_daily)
        counts["rainfall_daily.jsonl"] = write_jsonl(out_dir / "rainfall_daily.jsonl",
                                                     sorted(daily, key=lambda r: (r["date"], r["data_source"], r["zone_id"] or "", r["source_id"])))

    evs: list[dict] = []
    if "dfo" in sources:
        log.info("DFO flood archive (events intersecting the study box)")
        recs, f = events.download_dfo(_region_bbox(cfg), cache / "dfo", refresh)
        fetches["dfo"] = f
        evs += recs
        sections["dfo"] = {"events": len(recs), "first": min((r["began"] for r in recs), default=None),
                           "last": max((r["began"] for r in recs), default=None), **events.DFO_PROVENANCE.as_fields()}
    if "gdacs" in sources:
        log.info(f"GDACS events (TC, FL; India) {start} .. {end}")
        recs, f = events.download_gdacs(start, end, cache / "gdacs", refresh=refresh)
        fetches["gdacs"] = f
        r = cfg["region"]
        clat, clon = (r["min_lat"] + r["max_lat"]) / 2, (r["min_lon"] + r["max_lon"]) / 2
        for e in recs:
            if e["centroid_lat"] is not None:
                e["distance_to_region_km"] = round(haversine_km(clat, clon, e["centroid_lat"], e["centroid_lon"]), 1)
        evs += recs
        sections["gdacs"] = {"events": len(recs), "names": [e["name"] for e in recs], **events.GDACS_PROVENANCE.as_fields()}
    rerun_ev = [s for s in ("dfo", "gdacs") if s in sources]
    if rerun_ev:
        evs = _merge(out_dir / "official_events.jsonl", evs, rerun_ev)
        counts["official_events.jsonl"] = write_jsonl(out_dir / "official_events.jsonl",
                                                      sorted(evs, key=lambda e: (e["began"] or "", e["event_id"])))
    for fname in FILE_SOURCES:                       # files untouched by this run keep their counts
        if fname not in counts and (out_dir / fname).exists():
            counts[fname] = len(_load_jsonl(out_dir / fname))

    zones = [{"zone_id": z.zone_id, "row": z.row, "col": z.col, "min_lat": z.min_lat, "min_lon": z.min_lon,
              "max_lat": z.max_lat, "max_lon": z.max_lon, "center_lat": round(z.center[0], 6),
              "center_lon": round(z.center[1], 6)} for z in grid.zones()]
    write_json(data_root / "reference" / "zones.json", {"note": "analysis grid (not official boundaries)", "zones": zones})

    now = utc_iso(datetime.now(timezone.utc))
    manifest = {
        "data_mode": "real-historical", "synthetic": False, "generated_at": now,
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "region": cfg["region"]["name"], "sources": sections, "line_counts": counts,
        "fetches": fetches, "not_available": NOT_AVAILABLE,
        "note": "No injected defects: the injected-vs-detected check does not apply to real data.",
    }
    write_json(out_dir / "manifest.json", manifest)
    write_json(data_root / "dataset_manifest.json", {
        "data_mode": "real-historical", "synthetic": False, "generated_at": now,
        "period": manifest["period"], "sources": {k: {kk: v.get(kk) for kk in ("data_source", "observation_type", "licence")}
                                                  for k, v in sections.items()},
        "not_available": NOT_AVAILABLE,
        "label": "Real historical data (ERA5 reanalysis hourly rainfall; IMD/NOAA observed daily rainfall; DFO/GDACS events). "
                 "No live feed. Water level, traffic and reports are not available from verified real sources.",
    })
    log.info(f"real-data import complete: {counts}")
    return manifest


def main(argv=None) -> int:
    cfg = get_config()
    ap = argparse.ArgumentParser(description="Import verified real datasets (see docs/dataset_migration_plan.md)")
    ap.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    ap.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END)
    ap.add_argument("--sources", default=",".join(ALL_SOURCES), help=f"comma list from {ALL_SOURCES}")
    ap.add_argument("--data-root", default=str(PROJECT_ROOT / "data_real"))
    ap.add_argument("--refresh", action="store_true", help="re-download instead of using cached raw responses")
    args = ap.parse_args(argv)
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    bad = sorted(set(sources) - set(ALL_SOURCES))
    if bad:
        ap.error(f"unknown sources {bad}")
    if args.end >= date.today():
        ap.error("end date must be in the past (ERA5 lags real time by ~5 days)")
    try:
        m = run(cfg, Path(args.data_root), args.start, args.end, sources, args.refresh)
    except RealDataError as exc:
        log.error(f"real-data import FAILED (nothing substituted): {exc}")
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 2
    print(json.dumps({"ok": True, "data_root": args.data_root, "period": m["period"], "line_counts": m["line_counts"],
                      "sources": {k: {kk: v for kk, v in s.items() if kk in ("records", "events", "zones", "cells", "stations", "names", "era5_cells", "hours_missing_precipitation", "missing_days", "max_cell_distance_km")}
                                  for k, s in m["sources"].items()}}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
