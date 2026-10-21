"""Explainable, rule-based zone risk scoring.

    risk_score = 100 * sum(w_i * f_i, i available) / sum(w_i, i available)

* ``f_i`` is indicator ``i`` normalised to [0, 1] with a clamped linear ramp
  between the configured ``low`` and ``high`` values.
* Missing indicators are excluded from numerator and denominator and reduce
  ``coverage`` (available weight / total weight). A missing value is never
  treated as evidence of safety: with low coverage the confidence drops, and
  below the configured minimum the zone is reported as "Insufficient data"
  with no score.
* ``contributions`` are the points each indicator adds; they sum to the score.

This is a transparent demonstration index, not a validated disaster forecast.
The same function is used by Spark (wrapped in a UDF), the API and the tests,
so there is a single definition of the formula.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Mapping, Optional

INSUFFICIENT = "Insufficient data"

_DISPLAY = {
    "rainfall": ("rainfall", "{:.1f} mm/hr"),
    "water_level": ("water level", "{:.2f}x danger level"),
    "incidents": ("incident reports", "{:.1f} weighted reports"),
    "traffic": ("traffic disruption", "{:.0%} of observations"),
    "historical": ("historical hazard", "index {:.2f}"),
}


@dataclass
class RiskAssessment:
    risk_score: Optional[float]
    risk_category: str
    confidence: str
    coverage: float
    contributions: dict = field(default_factory=dict)
    normalized: dict = field(default_factory=dict)
    raw_values: dict = field(default_factory=dict)
    available_features: list = field(default_factory=list)
    missing_features: list = field(default_factory=list)
    explanation: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return True


def normalize(value: float, low: float, high: float) -> float:
    """Clamped linear ramp: ``low`` -> 0, ``high`` -> 1."""
    if high <= low:
        raise ValueError("high must exceed low")
    return max(0.0, min(1.0, (float(value) - low) / (high - low)))


def categorize(score: Optional[float], categories: Mapping[str, float]) -> str:
    if score is None:
        return INSUFFICIENT
    for name, lower in sorted(categories.items(), key=lambda kv: kv[1], reverse=True):
        if score >= lower:
            return name
    return min(categories.items(), key=lambda kv: kv[1])[0]


def confidence_level(coverage: float, thresholds: Mapping[str, float]) -> str:
    if coverage >= thresholds["high"]:
        return "high"
    if coverage >= thresholds["medium"]:
        return "medium"
    if coverage >= thresholds["low"]:
        return "low"
    return "insufficient"


def assess(indicators: Mapping[str, object], risk_cfg: Mapping) -> RiskAssessment:
    """Score one zone/time-window from raw indicator values.

    ``indicators`` maps the configured ``input`` names (e.g.
    ``rainfall_avg_mm_hr``) to values; ``None``/NaN means not observed.
    """
    feats = risk_cfg["features"]
    total_weight = sum(float(f["weight"]) for f in feats.values())
    available, missing = [], []
    normalized, raw_values, weighted = {}, {}, {}
    for name, f in feats.items():
        value = indicators.get(f["input"])
        if _is_missing(value) or float(f["weight"]) <= 0:
            missing.append(name)
            continue
        v = float(value)  # type: ignore[arg-type]
        raw_values[name] = round(v, 4)
        normalized[name] = round(normalize(v, float(f["low"]), float(f["high"])), 4)
        weighted[name] = float(f["weight"]) * normalize(v, float(f["low"]), float(f["high"]))
        available.append(name)

    active_weight = sum(float(feats[n]["weight"]) for n in available)
    coverage = round(active_weight / total_weight, 4) if total_weight else 0.0
    confidence = confidence_level(coverage, risk_cfg["confidence"])

    if not available or confidence == "insufficient":
        return RiskAssessment(
            risk_score=None, risk_category=INSUFFICIENT, confidence="insufficient",
            coverage=coverage, normalized=normalized, raw_values=raw_values,
            available_features=available, missing_features=missing,
            explanation=(f"Insufficient data: only {coverage:.0%} of indicator weight is observed "
                         f"(missing: {', '.join(missing) or 'none'}). No score is reported."),
        )

    contributions = {n: round(100.0 * weighted[n] / active_weight, 2) for n in available}
    score = round(max(0.0, min(100.0, sum(100.0 * weighted[n] / active_weight for n in available))), 2)
    category = categorize(score, risk_cfg["categories"])
    return RiskAssessment(
        risk_score=score, risk_category=category, confidence=confidence, coverage=coverage,
        contributions=contributions, normalized=normalized, raw_values=raw_values,
        available_features=available, missing_features=missing,
        explanation=explain(score, category, confidence, contributions, raw_values, missing),
    )


def explain(score: float, category: str, confidence: str, contributions: Mapping[str, float],
            raw_values: Mapping[str, float], missing: list) -> str:
    ranked = sorted(contributions.items(), key=lambda kv: kv[1], reverse=True)
    parts = []
    for name, pts in ranked:
        if pts <= 0:
            continue
        label, fmt = _DISPLAY.get(name, (name, "{}"))
        parts.append(f"{label} {fmt.format(raw_values[name])} (+{pts:.1f})")
    head = f"Score {score:.1f} ({category}, {confidence} confidence)."
    body = (" Main factors: " + "; ".join(parts[:3]) + ".") if parts else " No indicator is above its baseline."
    tail = f" Not observed: {', '.join(missing)}." if missing else ""
    return head + body + tail
