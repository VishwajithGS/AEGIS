"""Deliberate data defects for testing the validation pipeline.

The injector never marks a record as defective inside the payload; the ETL
must detect problems on its own. Instead every injection is counted in a
manifest so data-quality reports can be checked against what was injected.

Time-based defects (late or future timestamps) are produced by the
orchestrator in ``data_generator.py`` because they require re-encoding the
event time; field-level defects are produced here.
"""
from __future__ import annotations

import json
import random
from collections import Counter

# (type, relative weight). "duplicate" re-sends an identical record.
FIELD_ANOMALIES = [
    ("duplicate", 30),
    ("missing_required", 14),
    ("out_of_range", 14),
    ("invalid_type", 10),
    ("malformed_json", 10),
    ("invalid_coordinates", 6),
    ("outside_study_region", 6),
    ("unknown_schema_version", 4),
]
TIME_ANOMALIES = [("future_timestamp", 6)]
LATE_KINDS = [("late_within_watermark", 60), ("late_beyond_watermark", 40)]

# Required field removed by "missing_required" (besides time / id).
_REQUIRED = {
    "wx-metric-1": "rainfall_mm_hr", "wx-imperial-2": "precip_in_hr", "wl-1": "water_level",
    "tr-1": "avg_speed_kmph", "pr-1": "category", "hi-1": "category",
}
# Numeric field given a non-numeric value by "invalid_type".
_NUMERIC = {
    "wx-metric-1": "rainfall_mm_hr", "wx-imperial-2": "precip_in_hr", "wl-1": "water_level",
    "tr-1": "avg_speed_kmph", "pr-1": "confidence", "hi-1": "affected_area_km2",
}
# Fields and impossible values used by "out_of_range" (after unit conversion
# every value lies outside aegis.canonical.RANGES).
_OUT_OF_RANGE = {
    "wx-metric-1": [("rainfall_mm_hr", -4.0), ("humidity_pct", 140.0), ("temperature_c", 71.0)],
    "wx-imperial-2": [("precip_in_hr", 15.0), ("rh_pct", -12.0)],
    "wl-1": [("water_level", 9999.0)],
    "tr-1": [("avg_speed_kmph", 420.0), ("vehicle_density", -8.0)],
    "pr-1": [("confidence", 1.7)],
    "hi-1": [("affected_area_km2", -3.0)],
}
_TIME_FIELD = {"wx-imperial-2": "ts_ms"}
_ID_FIELD = {"wx-imperial-2": "obs_id"}


def pick(rng: random.Random, table: list[tuple[str, int]]) -> str:
    total = sum(w for _, w in table)
    x = rng.uniform(0, total)
    for name, w in table:
        x -= w
        if x <= 0:
            return name
    return table[-1][0]


class AnomalyInjector:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.counts: Counter = Counter()

    def apply(self, ev: dict, kind: str) -> str:
        """Return the serialized defective payload for ``kind``."""
        self.counts[kind] += 1
        version = str(ev.get("schema_version"))
        if version not in _REQUIRED:
            raise ValueError(f"unknown schema_version for anomaly injection: {version}")
        ev = json.loads(json.dumps(ev))  # deep copy
        if kind == "missing_required":
            target = self.rng.choice([_TIME_FIELD.get(version, "event_time"), _REQUIRED[version],
                                      _ID_FIELD.get(version, "event_id")])
            ev.pop(target, None)
        elif kind == "out_of_range":
            field, value = self.rng.choice(_OUT_OF_RANGE[version])
            ev[field] = value
        elif kind == "invalid_type":
            ev[_NUMERIC[version]] = self.rng.choice(["heavy", "N/A", "--", "unknown"])
        elif kind == "invalid_coordinates":
            if "location" in ev:
                ev["location"]["latitude"] = 95.5
            else:
                ev["lat"] = 95.5
        elif kind == "outside_study_region":
            if "location" in ev:
                ev["location"]["latitude"] = 14.20
            else:
                ev["lat"] = 14.20
        elif kind == "unknown_schema_version":
            ev["schema_version"] = version + "-beta"
        elif kind == "malformed_json":
            raw = json.dumps(ev)
            return raw[: max(10, int(len(raw) * self.rng.uniform(0.3, 0.8)))]
        elif kind == "duplicate":
            pass
        else:
            raise ValueError(f"unknown anomaly kind: {kind}")
        return json.dumps(ev)
