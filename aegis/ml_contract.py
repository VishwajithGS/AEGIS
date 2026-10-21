"""Feature contract of the flood-severity classifier (pure Python).

Shared by the Spark feature builder, the inference wrapper and the API so the
names, units and validation rules are defined once.
"""
from __future__ import annotations

import math

FEATURE_COLUMNS = [
    "rain_3h_mm", "rain_max_1h_mm_hr", "level_ratio_max_3h", "humidity_avg_3h",
    "disrupted_ratio_1h", "speed_ratio_avg_1h", "reports_6h",
    "water_obs_available", "traffic_obs_available",
]

FEATURE_DOCS = {
    "rain_3h_mm": ("mm", "rainfall accumulated over the 3 hours before the assessment time"),
    "rain_max_1h_mm_hr": ("mm/hr", "peak rainfall intensity in the previous hour"),
    "level_ratio_max_3h": ("ratio", "max river level / danger level over 3 hours (nearest gauge)"),
    "humidity_avg_3h": ("%", "mean relative humidity over 3 hours"),
    "disrupted_ratio_1h": ("0-1", "share of disrupted traffic observations in the previous hour"),
    "speed_ratio_avg_1h": ("0-1", "mean traffic speed / free-flow speed in the previous hour"),
    "reports_6h": ("count", "non-rejected public incident reports in the zone in the previous 6 hours"),
    "water_obs_available": ("0/1", "1 if any river-gauge reading was available in the 3 hours"),
    "traffic_obs_available": ("0/1", "1 if any traffic reading was available in the previous hour"),
}

FEATURE_BOUNDS = {
    "rain_3h_mm": (0.0, 900.0), "rain_max_1h_mm_hr": (0.0, 300.0), "level_ratio_max_3h": (0.0, 10.0),
    "humidity_avg_3h": (0.0, 100.0), "disrupted_ratio_1h": (0.0, 1.0), "speed_ratio_avg_1h": (0.0, 2.0),
    "reports_6h": (0.0, 10000.0), "water_obs_available": (0.0, 1.0), "traffic_obs_available": (0.0, 1.0),
}
REQUIRED = {"reports_6h", "water_obs_available", "traffic_obs_available"}
BINARY = {"water_obs_available", "traffic_obs_available"}
PASSTHROUGH = {"zone_id", "input_source"}


class FeatureValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def validate_features(record: dict, features: list[str] | None = None) -> dict:
    """Return a clean ``{feature: float | None}`` or raise FeatureValidationError.

    Optional measurements may be NULL (the model pipeline imputes training
    medians and the availability flags expose the missingness)."""
    features = features or FEATURE_COLUMNS
    if not isinstance(record, dict):
        raise FeatureValidationError(["feature record must be a JSON object"])
    errors, clean = [], {}
    unknown = sorted(set(record) - set(features) - PASSTHROUGH)
    if unknown:
        errors.append(f"unknown fields: {unknown}")
    for f in features:
        v = record.get(f)
        if v is None:
            if f in REQUIRED:
                errors.append(f"{f} is required")
            clean[f] = None
            continue
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            errors.append(f"{f} must be a number")
            continue
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            errors.append(f"{f} must be finite")
            continue
        lo, hi = FEATURE_BOUNDS.get(f, (-math.inf, math.inf))
        if not lo <= fv <= hi:
            errors.append(f"{f}={v} outside [{lo}, {hi}]")
            continue
        if f in BINARY and fv not in (0.0, 1.0):
            errors.append(f"{f} must be 0 or 1")
            continue
        clean[f] = fv
    if errors:
        raise FeatureValidationError(errors)
    return clean
