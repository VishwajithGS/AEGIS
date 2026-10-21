import math

import pytest

from aegis.risk import INSUFFICIENT, assess, categorize, confidence_level, normalize

ALL = {"rainfall_avg_mm_hr": 26.25, "level_ratio_max": 0.8, "weighted_reports": 2.5,
       "disrupted_ratio": 0.375, "historical_hazard_index": 0.5}   # every indicator exactly at mid-ramp


@pytest.fixture()
def risk_cfg(cfg):
    return cfg["risk"]


def test_normalize_clamps_and_ramps():
    assert normalize(-5, 0, 10) == 0.0
    assert normalize(5, 0, 10) == 0.5
    assert normalize(50, 0, 10) == 1.0
    with pytest.raises(ValueError):
        normalize(1, 5, 5)


def test_categories_and_confidence(risk_cfg):
    cats = risk_cfg["categories"]
    assert categorize(0, cats) == "Low"
    assert categorize(24.99, cats) == "Low"
    assert categorize(25, cats) == "Moderate"
    assert categorize(50, cats) == "High"
    assert categorize(75, cats) == "Critical"
    assert categorize(None, cats) == INSUFFICIENT
    c = risk_cfg["confidence"]
    assert confidence_level(1.0, c) == "high"
    assert confidence_level(0.7, c) == "medium"
    assert confidence_level(0.45, c) == "low"
    assert confidence_level(0.2, c) == "insufficient"


def test_all_indicators_mid_ramp_gives_50(risk_cfg):
    a = assess(ALL, risk_cfg)
    assert a.risk_score == pytest.approx(50.0)
    assert a.risk_category == "High" and a.confidence == "high" and a.coverage == 1.0
    assert sum(a.contributions.values()) == pytest.approx(a.risk_score, abs=0.05)
    assert a.missing_features == []


def test_missing_data_is_not_treated_as_safe(risk_cfg):
    # Heavy rain observed, everything else unknown: the score reflects the rain,
    # it is NOT diluted towards "Low" by the missing indicators ...
    a = assess({"rainfall_avg_mm_hr": 50.0, "weighted_reports": 5.0}, risk_cfg)
    assert a.risk_score == pytest.approx(100.0)
    # ... and the reduced coverage is reported as reduced confidence.
    assert a.coverage == pytest.approx(0.5)
    assert a.confidence == "low"
    assert set(a.missing_features) == {"water_level", "traffic", "historical"}
    assert "Not observed" in a.explanation


def test_insufficient_data_returns_no_score(risk_cfg):
    a = assess({"historical_hazard_index": 1.0}, risk_cfg)        # 10% of the weight only
    assert a.risk_score is None and a.risk_category == INSUFFICIENT and a.confidence == "insufficient"
    b = assess({}, risk_cfg)
    assert b.risk_score is None and b.coverage == 0.0


def test_nan_and_none_are_missing(risk_cfg):
    a = assess({**ALL, "level_ratio_max": float("nan"), "disrupted_ratio": None}, risk_cfg)
    assert {"water_level", "traffic"} <= set(a.missing_features)
    assert not math.isnan(a.risk_score)


def test_score_bounds_and_explanation(risk_cfg):
    hi = assess({k: 1e6 for k in ALL}, risk_cfg)
    lo = assess({k: -1e6 for k in ALL}, risk_cfg)
    assert hi.risk_score == 100.0 and hi.risk_category == "Critical"
    assert lo.risk_score == 0.0 and lo.risk_category == "Low"
    a = assess({**ALL, "rainfall_avg_mm_hr": 50.0}, risk_cfg)
    assert a.explanation.startswith("Score") and "rainfall" in a.explanation.split("Main factors:")[1].split(";")[0]


def test_weights_are_configurable(risk_cfg):
    cfg2 = {**risk_cfg, "features": {k: dict(v) for k, v in risk_cfg["features"].items()}}
    cfg2["features"]["rainfall"]["weight"] = 0.0          # disable rainfall
    a = assess({**ALL, "rainfall_avg_mm_hr": 50.0}, cfg2)
    assert "rainfall" in a.missing_features
    assert a.risk_score == pytest.approx(50.0)
