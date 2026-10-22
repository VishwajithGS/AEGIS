"""Canonical vocabularies, unit conversions and validation ranges.

These tables are the single definition of the source-to-canonical mapping
rules. Spark transforms (``spark_jobs/transforms.py``) build their
expressions from them and the data dictionary documents them.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Source formats
# ---------------------------------------------------------------------------
SCHEMA_VERSIONS = {
    "weather": ["wx-metric-1", "wx-imperial-2", "wx-era5-1"],   # wx-era5-1 = real ERA5 reanalysis (aegis/realdata)
    "water_level": ["wl-1"],
    "traffic": ["tr-1"],
    "public_report": ["pr-1"],
    "historical_incident": ["hi-1"],
}

TOPIC_SOURCE_TYPES = {
    "weather": ["weather"],
    "water_level": ["water_level"],
    "traffic": ["traffic"],
    "incidents": ["public_report", "historical_incident"],
}

# ---------------------------------------------------------------------------
# Units -> canonical (metres, mm/hr, degrees Celsius)
# ---------------------------------------------------------------------------
LENGTH_TO_METRES = {"m": 1.0, "cm": 0.01, "mm": 0.001, "ft": 0.3048}
INCH_TO_MM = 25.4

# ---------------------------------------------------------------------------
# Category normalisation (lower-cased, trimmed source value -> canonical)
# ---------------------------------------------------------------------------
WEATHER_CONDITIONS = {
    "clear": "clear", "clr": "clear", "sunny": "clear", "skc": "clear",
    "cloudy": "cloudy", "overcast": "cloudy", "ovc": "cloudy", "partly cloudy": "cloudy", "bkn": "cloudy",
    "drizzle": "light_rain", "light rain": "light_rain", "light_rain": "light_rain", "-ra": "light_rain", "dz": "light_rain",
    "rain": "moderate_rain", "moderate rain": "moderate_rain", "moderate_rain": "moderate_rain", "ra": "moderate_rain",
    "heavy rain": "heavy_rain", "heavy_rain": "heavy_rain", "+ra": "heavy_rain", "very heavy rain": "heavy_rain",
    "thunderstorm": "thunderstorm", "tsra": "thunderstorm", "+tsra": "thunderstorm", "storm": "thunderstorm",
}
WEATHER_CONDITION_VALUES = sorted(set(WEATHER_CONDITIONS.values()) | {"unknown"})

SENSOR_STATUS = {
    "ok": "ok", "normal": "ok", "good": "ok", "online": "ok",
    "degraded": "degraded", "warn": "degraded", "warning": "degraded", "low_battery": "degraded",
    "fault": "fault", "error": "fault", "offline": "fault", "failed": "fault",
}

TRAFFIC_DISRUPTION = {
    "none": "none", "free": "none", "normal": "none", "clear": "none",
    "slow": "slow", "congested": "slow", "heavy": "slow",
    "blocked": "blocked", "closed": "blocked", "closure": "blocked",
    "flooded": "flooded", "waterlogged": "flooded", "inundated": "flooded",
}
# A traffic observation counts as disrupted for risk analytics when its level is
# blocked/flooded or its speed falls below this share of free-flow speed.
DISRUPTED_LEVELS = ("blocked", "flooded")
DISRUPTED_SPEED_RATIO = 0.4

INCIDENT_CATEGORIES = {
    "flood": "flood", "flooding": "flood", "flash flood": "flood", "inundation": "flood", "river overflow": "flood",
    "waterlogging": "waterlogging", "water logging": "waterlogging", "water-logging": "waterlogging",
    "waterlogged road": "waterlogging", "stagnant water": "waterlogging", "waterlogged": "waterlogging",
    "tree fall": "tree_fall", "tree_fall": "tree_fall", "fallen tree": "tree_fall", "uprooted tree": "tree_fall",
    "power outage": "power_outage", "power_outage": "power_outage", "power cut": "power_outage", "blackout": "power_outage",
    "road blockage": "road_blockage", "road_blockage": "road_blockage", "road blocked": "road_blockage", "road closed": "road_blockage",
    "structure damage": "structure_damage", "structure_damage": "structure_damage", "wall collapse": "structure_damage",
}
INCIDENT_CATEGORY_VALUES = sorted(set(INCIDENT_CATEGORIES.values()) | {"other"})
FLOOD_RELATED_CATEGORIES = ("flood", "waterlogging")

VERIFICATION_STATUS = {
    "verified": "verified", "confirmed": "verified",
    "unverified": "unverified", "pending": "unverified", "new": "unverified",
    "rejected": "rejected", "false": "rejected", "duplicate": "rejected", "spam": "rejected",
}

SEVERITY_LABELS = ["Low", "Moderate", "High", "Critical"]
SEVERITY_NORMALIZE = {"low": "Low", "minor": "Low", "moderate": "Moderate", "medium": "Moderate",
                      "high": "High", "major": "High", "severe": "High", "critical": "Critical", "extreme": "Critical"}

# ---------------------------------------------------------------------------
# Physical plausibility ranges for canonical values (inclusive)
# ---------------------------------------------------------------------------
RANGES = {
    "temperature_c": (-20.0, 55.0),
    "humidity_pct": (0.0, 100.0),
    "rainfall_mm_hr": (0.0, 300.0),
    "water_level_m": (-5.0, 30.0),
    "danger_level_m": (0.1, 30.0),
    "density_veh_per_km": (0.0, 400.0),
    "avg_speed_kmph": (0.0, 160.0),
    "free_flow_speed_kmph": (5.0, 160.0),
    "confidence": (0.0, 1.0),
    "affected_area_km2": (0.0, 500.0),
}

# Rejection reason codes written to the quarantine dataset.
REASONS = [
    "malformed_json", "unknown_source_type", "unknown_schema_version", "topic_source_mismatch",
    "missing_required", "invalid_type", "invalid_timestamp", "future_timestamp",
    "invalid_coordinates", "outside_study_region", "out_of_range", "invalid_unit",
]

# Non-fatal data-quality flags kept on valid records.
FLAGS = [
    "late_arrival", "sensor_degraded", "sensor_fault", "unknown_category", "unknown_condition",
    "missing_coordinates", "missing_optional", "unknown_status",
]
