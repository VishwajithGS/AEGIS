"""Spark MLlib severity classifier: schema checks, training, metrics, persistence, inference."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.spark
LABELS = ["Low", "Moderate", "High", "Critical"]


def synthetic_training(spark, n=800, seed=3):
    rng = random.Random(seed)
    t0 = datetime(2026, 9, 1)
    rows = []
    for i in range(n):
        rain = rng.uniform(0, 80)
        lvl = rng.uniform(0.3, 1.2) if rng.random() > 0.2 else None
        rep = float(rng.randint(0, 6))
        s = 0.5 * min(rain / 60, 1) + 0.35 * (min(max((lvl or 0.6) - 0.5, 0) / 0.6, 1)) + 0.15 * min(rep / 4, 1)
        s += rng.gauss(0, 0.03)
        label = "Low" if s < 0.25 else "Moderate" if s < 0.5 else "High" if s < 0.72 else "Critical"
        rows.append((f"INC-{i}", t0 + timedelta(minutes=30 * i), label, rain, rain * 0.6, lvl, 85.0,
                     rng.uniform(0, 1), rng.uniform(0.2, 1), rep, 0.0 if lvl is None else 1.0, 1.0))
    return spark.createDataFrame(rows, "incident_id string, event_time timestamp, label string, rain_3h_mm double, "
                                       "rain_max_1h_mm_hr double, level_ratio_max_3h double, humidity_avg_3h double, "
                                       "disrupted_ratio_1h double, speed_ratio_avg_1h double, reports_6h double, "
                                       "water_obs_available double, traffic_obs_available double")


@pytest.fixture(scope="module")
def trained(spark, tmp_path_factory):
    from aegis.config import load_config
    from spark_jobs.model_training import train
    root = tmp_path_factory.mktemp("model")
    cfg = load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(root / "data")})
    meta = train(spark, cfg, data=synthetic_training(spark), out_root=root / "models")
    return cfg, meta, root / "models"


def test_training_metrics_are_computed(trained):
    _, meta, _ = trained
    m = meta["metrics"]
    for k in ("accuracy", "weightedPrecision", "weightedRecall", "weighted_f1"):
        assert 0.0 <= m[k] <= 1.0
    cm = m["confusion_matrix"]["rows_actual_cols_predicted"]
    assert sum(map(sum, cm)) == m["test_rows"] == meta["split"]["test_rows"]
    assert set(m["per_class"]) == set(LABELS)
    assert m["accuracy"] > m["baseline_majority_class"]["accuracy"]      # learns more than the majority class
    assert meta["split"]["method"] == "time-based"
    assert "SYNTHETIC" in meta["label_provenance"]


def test_time_split_has_no_overlap(spark):
    from spark_jobs.model_training import label_indexer, time_split
    df = label_indexer(LABELS).transform(synthetic_training(spark, 300))
    train, test, cutoff = time_split(df, 0.2)
    assert train.agg({"event_time": "max"}).first()[0] < test.agg({"event_time": "min"}).first()[0]


def test_saved_model_reloads_and_predicts(spark, trained):
    _, meta, root = trained
    assert meta["reload_check"]["passed"]
    from spark_jobs.model_inference import SeverityModel
    model = SeverityModel(spark, root / meta["version"])
    out = model.predict_records([
        {"rain_3h_mm": 75, "rain_max_1h_mm_hr": 45, "level_ratio_max_3h": 1.1, "humidity_avg_3h": 97,
         "disrupted_ratio_1h": 0.8, "speed_ratio_avg_1h": 0.2, "reports_6h": 6, "water_obs_available": 1,
         "traffic_obs_available": 1},
        {"rain_3h_mm": 0, "rain_max_1h_mm_hr": 0, "level_ratio_max_3h": None, "humidity_avg_3h": None,
         "disrupted_ratio_1h": None, "speed_ratio_avg_1h": None, "reports_6h": 0, "water_obs_available": 0,
         "traffic_obs_available": 0}])
    assert [o["predicted_severity"] in LABELS for o in out] == [True, True]
    assert out[0]["predicted_severity"] in ("High", "Critical") and out[1]["predicted_severity"] == "Low"
    assert abs(sum(out[0]["probabilities"].values()) - 1.0) < 1e-3
    assert out[1]["imputed_features"] == ["level_ratio_max_3h", "humidity_avg_3h", "disrupted_ratio_1h", "speed_ratio_avg_1h"]


def test_invalid_training_data_rejected(spark, cfg):
    from spark_jobs.model_training import TrainingDataError, validate_training_frame
    df = synthetic_training(spark, 50)
    feats = list(cfg["model"]["features"])
    with pytest.raises(TrainingDataError, match="leaking"):
        validate_training_frame(df, feats + ["label"], LABELS, 10)
    with pytest.raises(TrainingDataError, match="at least"):
        validate_training_frame(df, feats, LABELS, 1000)
    with pytest.raises(TrainingDataError, match="missing columns"):
        validate_training_frame(df.drop("reports_6h"), feats, LABELS, 10)
    with pytest.raises(TrainingDataError, match="unexpected labels"):
        validate_training_frame(df.withColumn("label", df["label"].substr(1, 2)), feats, LABELS, 10)


def test_missing_model_reports_unavailable(spark, tmp_path):
    from spark_jobs.model_inference import ModelUnavailable, SeverityModel
    with pytest.raises(ModelUnavailable):
        SeverityModel(spark, tmp_path / "nothing-here")
