import pytest

from aegis.ml_contract import FEATURE_COLUMNS, FeatureValidationError, validate_features

GOOD = {"rain_3h_mm": 40.0, "rain_max_1h_mm_hr": 25, "level_ratio_max_3h": 0.9, "humidity_avg_3h": 95,
        "disrupted_ratio_1h": 0.4, "speed_ratio_avg_1h": 0.5, "reports_6h": 3, "water_obs_available": 1,
        "traffic_obs_available": 1}


def test_valid_record_is_cleaned_to_floats():
    out = validate_features(GOOD)
    assert list(out) == FEATURE_COLUMNS and all(isinstance(v, float) for v in out.values())


def test_optional_measurements_may_be_null():
    out = validate_features({**GOOD, "level_ratio_max_3h": None, "water_obs_available": 0})
    assert out["level_ratio_max_3h"] is None


@pytest.mark.parametrize("patch,msg", [
    ({"reports_6h": None}, "reports_6h is required"),
    ({"rain_3h_mm": -1}, "outside"),
    ({"rain_3h_mm": "lots"}, "must be a number"),
    ({"rain_3h_mm": float("nan")}, "finite"),
    ({"water_obs_available": 0.5}, "0 or 1"),
    ({"severity": "High"}, "unknown fields"),            # the label can never be supplied as an input
    ({"traffic_obs_available": True}, "must be a number"),
])
def test_invalid_records_rejected(patch, msg):
    with pytest.raises(FeatureValidationError) as e:
        validate_features({**GOOD, **patch})
    assert msg in str(e.value)


def test_non_dict_rejected():
    with pytest.raises(FeatureValidationError):
        validate_features(["not", "a", "dict"])  # type: ignore[arg-type]
