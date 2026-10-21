"""Encoders for the heterogeneous raw source formats.

Each source deliberately uses different field names, units and timestamp
representations so the ETL has real mapping work to do:

=================  ==========================  =================================
schema_version     timestamp                   notable differences
=================  ==========================  =================================
wx-metric-1        ISO-8601, +05:30 offset     metric units, flat lat/lon
wx-imperial-2      epoch milliseconds (ts_ms)  degF, inches/hr, nested location,
                                               keys obs_id/station, METAR codes
wl-1               ISO-8601 UTC ("Z")          level in m / cm / ft per sensor
tr-1               "yyyy-MM-dd HH:mm:ss" local speed + density, text disruption
                   (Asia/Kolkata, no offset)
pr-1               ISO-8601, +05:30 offset     free-text category, optional coords
hi-1               ISO-8601 UTC ("Z")          incident archive with severity label
=================  ==========================  =================================

Every record carries ``"synthetic": true``.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30), name="IST")  # fixed offset, no DST

CONDITION_TEXT = {
    "clear": ["Clear", "clear", "Sunny"],
    "cloudy": ["Cloudy", "Overcast", "partly cloudy"],
    "light_rain": ["Light Rain", "drizzle", "light_rain"],
    "moderate_rain": ["Rain", "Moderate Rain", "moderate_rain"],
    "heavy_rain": ["Heavy Rain", "heavy_rain", "Very Heavy Rain"],
    "thunderstorm": ["Thunderstorm", "thunderstorm"],
}
METAR_CODES = {
    "clear": ["CLR", "SKC"], "cloudy": ["OVC", "BKN"], "light_rain": ["-RA", "DZ"],
    "moderate_rain": ["RA"], "heavy_rain": ["+RA"], "thunderstorm": ["TSRA", "+TSRA"],
}
CATEGORY_TEXT = {
    "flood": ["Flooding", "flood", "Flash flood", "River overflow", "Inundation"],
    "waterlogging": ["Water logging", "Waterlogged road", "waterlogging", "Stagnant water"],
    "tree_fall": ["Tree fall", "Fallen tree", "Uprooted tree"],
    "power_outage": ["Power outage", "Power cut", "blackout"],
    "road_blockage": ["Road blocked", "Road closed", "road_blockage"],
    "structure_damage": ["Wall collapse", "Structure damage"],
}
DESCRIPTIONS = {
    "flood": "Water entering homes along the low-lying street (synthetic report)",
    "waterlogging": "Knee-deep water on the road; vehicles stalling (synthetic report)",
    "tree_fall": "Tree has fallen across the lane (synthetic report)",
    "power_outage": "Power supply interrupted in the neighbourhood (synthetic report)",
    "road_blockage": "Road impassable due to debris (synthetic report)",
    "structure_damage": "Compound wall collapsed after heavy rain (synthetic report)",
}


def condition_from_rain(rain_mm_hr: float, rng: random.Random) -> str:
    if rain_mm_hr >= 35 and rng.random() < 0.5:
        return "thunderstorm"
    if rain_mm_hr >= 15:
        return "heavy_rain"
    if rain_mm_hr >= 4:
        return "moderate_rain"
    if rain_mm_hr >= 0.5:
        return "light_rain"
    return "cloudy" if rng.random() < 0.5 else "clear"


def iso_ist(t: float) -> str:
    return datetime.fromtimestamp(t, IST).isoformat(timespec="milliseconds")


def iso_utc(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def local_naive(t: float) -> str:
    return datetime.fromtimestamp(t, IST).strftime("%Y-%m-%d %H:%M:%S")


def weather_event(event_id: str, station: dict, t: float, temp_c: float, humidity: float,
                  rain_mm_hr: float, rng: random.Random) -> dict:
    cond = condition_from_rain(rain_mm_hr, rng)
    if station["format"] == "wx-imperial-2":
        return {
            "obs_id": event_id, "source_type": "weather", "station": station["station_id"],
            "schema_version": "wx-imperial-2", "synthetic": True,
            "ts_ms": int(round(t * 1000)),
            "location": {"latitude": station["lat"], "longitude": station["lon"]},
            "temp_f": round(temp_c * 9.0 / 5.0 + 32.0, 1),
            "rh_pct": round(humidity, 1),
            "precip_in_hr": round(rain_mm_hr / 25.4, 3),
            "sky": rng.choice(METAR_CODES[cond]),
        }
    return {
        "event_id": event_id, "source_type": "weather", "source_id": station["station_id"],
        "schema_version": "wx-metric-1", "synthetic": True,
        "event_time": iso_ist(t), "lat": station["lat"], "lon": station["lon"],
        "temperature_c": round(temp_c, 2), "humidity_pct": round(humidity, 1),
        "rainfall_mm_hr": round(rain_mm_hr, 2), "condition": rng.choice(CONDITION_TEXT[cond]),
    }


_TO_UNIT = {"m": 1.0, "cm": 100.0, "ft": 1.0 / 0.3048, "mm": 1000.0}


def water_event(event_id: str, station: dict, t: float, level_m: float, status: str) -> dict:
    unit = station["unit"]
    return {
        "event_id": event_id, "source_type": "water_level", "source_id": station["station_id"],
        "schema_version": "wl-1", "synthetic": True, "event_time": iso_utc(t),
        "lat": station["lat"], "lon": station["lon"], "river_id": station["river_id"],
        "station_name": station["name"],
        "water_level": round(level_m * _TO_UNIT[unit], 3), "unit": unit,
        "danger_level": station["danger_level_m"], "danger_level_unit": "m",
        "sensor_status": status,
    }


def traffic_event(event_id: str, seg: dict, t: float, density: float, speed: float, disruption: str) -> dict:
    return {
        "event_id": event_id, "source_type": "traffic", "source_id": seg["source_id"],
        "schema_version": "tr-1", "synthetic": True, "event_time": local_naive(t),
        "segment_id": seg["segment_id"], "lat": seg["lat"], "lon": seg["lon"],
        "vehicle_density": round(density, 1), "avg_speed_kmph": round(speed, 1),
        "free_flow_speed_kmph": seg["free_flow_kmph"], "disruption": disruption,
    }


def public_report_event(event_id: str, t: float, lat: float | None, lon: float | None, category: str,
                        channel: str, verification: str, confidence: float, rng: random.Random) -> dict:
    ev = {
        "event_id": event_id, "source_type": "public_report", "source_id": f"citizen-{channel}",
        "schema_version": "pr-1", "synthetic": True, "event_time": iso_ist(t),
        "report_id": event_id, "lat": lat, "lon": lon,
        "category": rng.choice(CATEGORY_TEXT.get(category, ["Other"])),
        "description": DESCRIPTIONS.get(category, "Unclassified hazard (synthetic report)"),
        "channel": channel, "verification_status": verification, "confidence": round(confidence, 2),
    }
    return ev


def historical_incident_event(event_id: str, incident_id: str, t: float, lat: float, lon: float,
                              category: str, severity: str, affected_road: str | None,
                              affected_area_km2: float) -> dict:
    return {
        "event_id": event_id, "source_type": "historical_incident", "source_id": "aegis-sim-archive",
        "schema_version": "hi-1", "synthetic": True, "event_time": iso_utc(t),
        "incident_id": incident_id, "lat": round(lat, 5), "lon": round(lon, 5),
        "category": category, "severity": severity,
        "description": f"Archived {category.replace('_', ' ')} incident (synthetic record)",
        "affected_road": affected_road, "affected_area_km2": round(affected_area_km2, 3),
    }


def event_key(ev: dict) -> str:
    """Kafka message key: the source id (keeps per-source ordering)."""
    return str(ev.get("source_id") or ev.get("station") or ev.get("source_type") or "unknown")


def event_identity(ev: dict) -> str:
    return str(ev.get("event_id") or ev.get("obs_id") or "")
