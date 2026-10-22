"""Officially reported disaster events (regional), kept separate from incidents.

* Dartmouth Flood Observatory (DFO) Global Active Archive of Large Flood
  Events, ArcGIS feature service ``Global_Archive_of_large_flood_events``.
  Verified 2026-10-22: 24 events 1989–2015 intersect the AEGIS box, including
  ID 4309 (2015-11-10 – 2015-12-04, severity 2). Severity classes: 1 large,
  1.5 very large, 2 extreme. Polygons are regional (tens of thousands of km²).
* GDACS event API (UN OCHA / EC JRC). Verified: Tropical Cyclone MICHAUNG-23,
  2023-12-03 – 12-04, alert level Orange.

These are **events**, not incidents: they have no street-level location and
cover every AEGIS zone at once, so they are never written to the incident
tables, never used as zone-level hazard or as ML labels.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

from aegis.realdata.common import Provenance, RealDataError, fetch, require_fields, stable_id, utc_iso

DFO_SERVICE = ("https://services1.arcgis.com/AXaYBvnJsB5Q7sDF/ArcGIS/rest/services/"
               "Global_Archive_of_large_flood_events/FeatureServer/0/query")
DFO_PROVENANCE = Provenance(
    data_source="dfo-flood-archive", observation_type="official_event",
    source_url="https://floodobservatory.colorado.edu/Archives/index.html",
    licence="free for scholarly research and education with attribution: G.R. Brakenridge, Global Active Archive of Large Flood Events, DFO, Univ. of Colorado",
)
GDACS_API = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
GDACS_PROVENANCE = Provenance(
    data_source="gdacs", observation_type="official_event",
    source_url="https://www.gdacs.org/",
    licence="reuse with credit 'Global Disaster Alert and Coordination System, GDACS' (CC BY 4.0 per JRC); automatic alerts, not national warnings",
)
SCHEMA_VERSION = "ev-official-1"
DFO_SEVERITY = {1: "large", 1.5: "very large", 2: "extreme"}


def _ms_to_iso(ms) -> str | None:
    return None if ms is None else utc_iso(datetime.fromtimestamp(ms / 1000, timezone.utc))


def dfo_query_params(bbox: tuple[float, float, float, float]) -> dict:
    min_lon, min_lat, max_lon, max_lat = bbox
    return {"where": "1=1", "geometry": f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "geometryType": "esriGeometryEnvelope", "inSR": "4326", "spatialRel": "esriSpatialRelIntersects",
            "outFields": "ID,GLIDENUMBE,COUNTRY,OTHERCOUNT,LONG,LAT,AREA,BEGAN,ENDED,VALIDATION,DEAD,DISPLACED,MAINCAUSE,SEVERITY",
            "returnGeometry": "false", "f": "json"}


def parse_dfo(body: dict, retrieved_at: str) -> list[dict]:
    if "error" in body:
        raise RealDataError(f"DFO service error: {body['error']}")
    require_fields(body, ("features",), "DFO query")
    out = []
    for f in body["features"]:
        a = f.get("attributes") or {}
        require_fields(a, ("ID", "BEGAN", "SEVERITY"), "DFO feature")
        sev = a.get("SEVERITY")
        out.append({"event_id": stable_id("EVT", "dfo", a["ID"]), "record_type": "official_event",
                    "schema_version": SCHEMA_VERSION, "synthetic": False,
                    "source_event_id": f"DFO-{a['ID']}", "event_kind": "flood",
                    "name": f"DFO flood {a['ID']} ({(a.get('MAINCAUSE') or '').strip()})",
                    "began": _ms_to_iso(a.get("BEGAN")), "ended": _ms_to_iso(a.get("ENDED")),
                    "severity_class": sev, "severity_label": DFO_SEVERITY.get(sev),
                    "alert_level": None, "dead": a.get("DEAD"), "displaced": a.get("DISPLACED"),
                    "main_cause": (a.get("MAINCAUSE") or "").strip() or None,
                    "country": (a.get("COUNTRY") or "").strip() or None,
                    "affected_area_km2": a.get("AREA"), "centroid_lat": a.get("LAT"), "centroid_lon": a.get("LONG"),
                    "glide": (a.get("GLIDENUMBE") or "").strip() or None,
                    "validation": a.get("VALIDATION"), "spatial_scope": "regional polygon (intersects study box)",
                    "retrieved_at": retrieved_at, **DFO_PROVENANCE.as_fields()})
    return out


def parse_gdacs(body: dict, retrieved_at: str) -> list[dict]:
    require_fields(body, ("features",), "GDACS event list")
    out = []
    for f in body["features"]:
        p = f.get("properties") or {}
        require_fields(p, ("eventtype", "eventid", "fromdate", "alertlevel"), "GDACS event")
        coords = (f.get("geometry") or {}).get("coordinates") or [None, None]
        out.append({"event_id": stable_id("EVT", "gdacs", p["eventtype"], p["eventid"]), "record_type": "official_event",
                    "schema_version": SCHEMA_VERSION, "synthetic": False,
                    "source_event_id": f"GDACS-{p['eventtype']}-{p['eventid']}",
                    "event_kind": {"TC": "tropical_cyclone", "FL": "flood"}.get(p["eventtype"], p["eventtype"]),
                    "name": p.get("name") or p.get("eventname"),
                    "began": utc_iso(datetime.fromisoformat(p["fromdate"]).replace(tzinfo=timezone.utc)),
                    "ended": utc_iso(datetime.fromisoformat(p["todate"]).replace(tzinfo=timezone.utc)) if p.get("todate") else None,
                    "severity_class": None, "severity_label": None, "alert_level": p.get("alertlevel"),
                    "alert_score": p.get("alertscore"), "severity_text": (p.get("severitydata") or {}).get("severitytext"),
                    "country": p.get("country"), "centroid_lat": coords[1], "centroid_lon": coords[0],
                    "glide": p.get("glide") or None, "spatial_scope": "event point (country-level search)",
                    "url": (p.get("url") or {}).get("report") if isinstance(p.get("url"), dict) else None,
                    "retrieved_at": retrieved_at, **GDACS_PROVENANCE.as_fields()})
    return out


DFO_BUFFER_DEG = 0.35   # DFO polygons are coarse: the 2015 Chennai flood (ID 4309) misses the exact box


def download_dfo(bbox: tuple[float, float, float, float], cache_dir: Path, refresh: bool = False,
                 buffer_deg: float = DFO_BUFFER_DEG):
    """Events whose polygon intersects the study box (``intersects_study_box = True``)
    plus those intersecting the box buffered by ``buffer_deg`` (flag False)."""
    raw, res = fetch(DFO_SERVICE, cache_dir, params=dfo_query_params(bbox), refresh=refresh, suffix=".json")
    exact = parse_dfo(json.loads(raw), res.retrieved_at)
    fetches = [res.manifest()]
    ids = {e["event_id"] for e in exact}
    for e in exact:
        e["intersects_study_box"] = True
    out = list(exact)
    if buffer_deg > 0:
        b = (bbox[0] - buffer_deg, bbox[1] - buffer_deg, bbox[2] + buffer_deg, bbox[3] + buffer_deg)
        raw_b, res_b = fetch(DFO_SERVICE, cache_dir, params=dfo_query_params(b), refresh=refresh, suffix=".json")
        fetches.append(res_b.manifest())
        for e in parse_dfo(json.loads(raw_b), res_b.retrieved_at):
            if e["event_id"] not in ids:
                e["intersects_study_box"] = False
                e["spatial_scope"] = f"regional polygon within {buffer_deg} deg of the study box (not intersecting it)"
                out.append(e)
    return out, fetches


def download_gdacs(start: date, end: date, cache_dir: Path, country: str = "India", refresh: bool = False):
    params = {"eventlist": "TC;FL", "country": country, "fromDate": start.isoformat(), "toDate": end.isoformat()}
    raw, res = fetch(GDACS_API, cache_dir, params=params, refresh=refresh, suffix=".json")
    if not raw.strip():
        return [], [res.manifest()]          # GDACS returns an empty body when no event matches
    return parse_gdacs(json.loads(raw), res.retrieved_at), [res.manifest()]
