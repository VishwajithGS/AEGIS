"""Train and evaluate the supporting flood-incident severity classifier
(Spark MLlib, DataFrame-based ``pyspark.ml`` API).

Data      gold/ml_training_flood_severity (built by the batch ETL from the
          integrated sensor, traffic and report streams). LABELS ARE SYNTHETIC
          (producers.data_generator.severity_rule); metrics describe how well
          the tree recovers that synthetic rule from noisy observations, not
          real-world forecasting skill.
Split     time-based: the most recent ``model.test_fraction`` of incidents is
          held out, so the model never trains on incidents later than those it
          is tested on.
Pipeline  Imputer(median, fitted on the training split only)
          -> VectorAssembler -> DecisionTreeClassifier -> IndexToString.
          Labels are indexed with a FIXED mapping
          (StringIndexerModel.from_labels: Low=0, Moderate=1, High=2,
          Critical=3), so the test labels never influence preprocessing.
Outputs   data/models/flood_severity_dt/<version>/{pipeline/, metadata.json}
          data/models/flood_severity_dt/latest.json, data/reports/ml_evaluation.json

    python -m spark_jobs.model_training
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, IndexToString, StringIndexerModel, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aegis.config import data_path, get_config
from aegis.logging_config import setup_logging
from spark_jobs.common import Timer, build_spark, lake, path_exists, reports_dir, write_json
from spark_jobs.features import FEATURE_COLUMNS

log = setup_logging("ml-training")

# Columns that must never be model inputs (the answer or things derived from it).
FORBIDDEN_FEATURES = {"label", "label_idx", "severity", "incident_id", "event_time", "category", "zone_id",
                      "label_provenance", "prediction", "archived_incidents", "archived_flood_incidents"}
NUMERIC_TYPES = (T.DoubleType, T.FloatType, T.IntegerType, T.LongType, T.ShortType)


class TrainingDataError(ValueError):
    pass


def model_root(cfg: dict) -> Path:
    return data_path(cfg, "models_dir", cfg["model"]["name"])


def validate_training_frame(df: DataFrame, features: list[str], labels: list[str], min_rows: int) -> dict:
    """Schema / label checks before training. Raises TrainingDataError."""
    leak = sorted(set(features) & FORBIDDEN_FEATURES)
    if leak:
        raise TrainingDataError(f"target-leaking columns configured as features: {leak}")
    unknown = sorted(set(features) - set(FEATURE_COLUMNS))
    if unknown:
        raise TrainingDataError(f"features not produced by spark_jobs.features: {unknown}")
    missing = [c for c in features + ["label", "event_time"] if c not in df.columns]
    if missing:
        raise TrainingDataError(f"training data is missing columns: {missing}")
    types = {f.name: f.dataType for f in df.schema.fields}
    bad = [c for c in features if not isinstance(types[c], NUMERIC_TYPES)]
    if bad:
        raise TrainingDataError(f"non-numeric feature columns: {bad}")
    stats = df.agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("label").isNull().cast("int")).alias("null_labels")).first()
    if stats["n"] < min_rows:
        raise TrainingDataError(f"only {stats['n']} rows; at least {min_rows} required")
    if stats["null_labels"]:
        raise TrainingDataError(f"{stats['null_labels']} rows have a NULL label")
    seen = {r["label"] for r in df.select("label").distinct().collect()}
    if seen - set(labels):
        raise TrainingDataError(f"unexpected labels {sorted(seen - set(labels))}; allowed {labels}")
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in features]).first().asDict()
    return {"rows": int(stats["n"]), "labels_present": sorted(seen, key=labels.index), "null_counts": nulls}


def label_indexer(labels: list[str]) -> StringIndexerModel:
    return StringIndexerModel.from_labels(labels, inputCol="label", outputCol="label_idx", handleInvalid="error")


def time_split(df: DataFrame, test_fraction: float) -> tuple[DataFrame, DataFrame, datetime]:
    ts = df.withColumn("_ts", F.unix_timestamp("event_time"))
    cutoff = ts.approxQuantile("_ts", [1.0 - test_fraction], 0.0)[0]
    train = ts.filter(F.col("_ts") < cutoff).drop("_ts")
    test = ts.filter(F.col("_ts") >= cutoff).drop("_ts")
    return train, test, datetime.fromtimestamp(cutoff, timezone.utc)


def build_pipeline(features: list[str], labels: list[str], m: dict) -> Pipeline:
    imputed = [f"{c}__imp" for c in features]
    return Pipeline(stages=[
        Imputer(inputCols=features, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=imputed, outputCol="features", handleInvalid="error"),
        DecisionTreeClassifier(labelCol="label_idx", featuresCol="features", predictionCol="prediction",
                               maxDepth=int(m["max_depth"]), minInstancesPerNode=int(m["min_instances_per_node"]),
                               impurity=m["impurity"], seed=int(m["seed"])),
        IndexToString(inputCol="prediction", outputCol="predicted_label", labels=labels),
    ])


def evaluate(pred: DataFrame, labels: list[str], train_majority: Optional[str] = None) -> dict:
    ev = MulticlassClassificationEvaluator(labelCol="label_idx", predictionCol="prediction")
    out = {m: round(ev.setMetricName(m).evaluate(pred), 4)
           for m in ("accuracy", "weightedPrecision", "weightedRecall", "f1")}
    out["weighted_f1"] = out.pop("f1")
    support = {int(r["label_idx"]): int(r["count"]) for r in pred.groupBy("label_idx").count().collect()}
    cm = [[0] * len(labels) for _ in labels]
    for r in pred.groupBy("label_idx", "prediction").count().collect():
        cm[int(r["label_idx"])][int(r["prediction"])] = int(r["count"])
    # Per-class scores from the confusion matrix with Spark's MulticlassMetrics definitions (0.0 when a
    # denominator is 0). Spark's evaluator raises for a class absent from the test set; such a class has
    # no defined recall, so its scores are reported as null with support 0.
    per_class = {}
    for i, name in enumerate(labels):
        tp, predicted, actual = cm[i][i], sum(row[i] for row in cm), sum(cm[i])
        if actual == 0:
            per_class[name] = {"precision": None, "recall": None, "f1": None, "support": 0}
            continue
        p = tp / predicted if predicted else 0.0
        rc = tp / actual
        per_class[name] = {"precision": round(p, 4), "recall": round(rc, 4),
                           "f1": round(2 * p * rc / (p + rc), 4) if p + rc else 0.0, "support": actual}
    out["per_class"] = per_class
    out["confusion_matrix"] = {"labels": labels, "rows_actual_cols_predicted": cm}
    out["test_rows"] = sum(support.values())
    if train_majority is not None:
        n_major = support.get(labels.index(train_majority), 0)
        out["baseline_majority_class"] = {"class": train_majority,
                                          "accuracy": round(n_major / max(1, out["test_rows"]), 4)}
    out["classes_missing_from_test"] = [labels[i] for i in range(len(labels)) if support.get(i, 0) == 0]
    return out


def tree_summary(model: PipelineModel, features: list[str]) -> dict:
    dt = model.stages[2]
    imp = dt.featureImportances.toArray().tolist()
    return {"depth": dt.depth, "num_nodes": dt.numNodes,
            "feature_importances": dict(sorted(zip(features, [round(x, 4) for x in imp]), key=lambda kv: -kv[1])),
            "debug_string_head": "\n".join(dt.toDebugString.splitlines()[:40])}


def train(spark: SparkSession, cfg: dict, data: Optional[DataFrame] = None, out_root: Optional[Path] = None) -> dict:
    timer = Timer()
    m = cfg["model"]
    features, labels = list(m["features"]), list(m["labels"])
    if data is None:
        path = lake(cfg, "gold", "ml_training_flood_severity")
        if not path_exists(path):
            raise TrainingDataError(f"{path} not found; run the batch ETL first")
        data = spark.read.parquet(path)
    checks = validate_training_frame(data, features, labels, int(m["min_rows"]))
    indexed = label_indexer(labels).transform(data).cache()
    train_df, test_df, cutoff = time_split(indexed, float(m["test_fraction"]))
    n_train, n_test = train_df.count(), test_df.count()
    if n_train == 0 or n_test == 0:
        raise TrainingDataError(f"empty split: train={n_train} test={n_test}")
    dist = lambda d: {r["label"]: int(r["count"]) for r in d.groupBy("label").count().collect()}  # noqa: E731
    train_dist, test_dist = dist(train_df), dist(test_df)
    missing_train = sorted(set(labels) - set(train_dist))
    if missing_train:
        log.warning(f"classes absent from training split: {missing_train}")

    log.info(f"training DecisionTreeClassifier on {n_train} rows (test {n_test}, cutoff {cutoff.isoformat()})")
    t_fit = Timer()
    model = build_pipeline(features, labels, m).fit(train_df)
    fit_s = t_fit.seconds()
    pred = model.transform(test_df).cache()
    majority = max(train_dist.items(), key=lambda kv: kv[1])[0]
    metrics = evaluate(pred, labels, majority)

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = out_root or model_root(cfg)
    model_dir = root / version
    model.write().overwrite().save(str(model_dir / "pipeline"))

    # Reload and confirm the persisted model reproduces the in-memory predictions.
    reloaded = PipelineModel.load(str(model_dir / "pipeline"))
    sample = test_df.orderBy("incident_id").limit(25)
    a = {r["incident_id"]: r["predicted_label"] for r in model.transform(sample).select("incident_id", "predicted_label").collect()}
    b = {r["incident_id"]: r["predicted_label"] for r in reloaded.transform(sample).select("incident_id", "predicted_label").collect()}
    reload_ok = a == b and len(a) > 0

    meta = {
        "model_name": m["name"], "version": version, "algorithm": "pyspark.ml.classification.DecisionTreeClassifier",
        "spark_version": spark.version, "trained_at": datetime.now(timezone.utc).isoformat(),
        "features": features, "labels": labels, "label_index": {l: i for i, l in enumerate(labels)},
        "label_provenance": "SYNTHETIC - labels generated by producers.data_generator.severity_rule",
        "data_source": "gold/ml_training_flood_severity", "params": {k: m[k] for k in
                                                                     ("max_depth", "min_instances_per_node", "impurity", "seed")},
        "preprocessing": "Imputer(strategy=median) fitted on the training split; VectorAssembler",
        "split": {"method": "time-based", "test_fraction": m["test_fraction"], "cutoff_utc": cutoff.isoformat(),
                  "train_rows": n_train, "test_rows": n_test, "train_label_distribution": train_dist,
                  "test_label_distribution": test_dist},
        "data_checks": checks, "metrics": metrics, "tree": tree_summary(model, features),
        "reload_check": {"passed": reload_ok, "rows_compared": len(a)},
        "durations_s": {"fit": fit_s, "total": timer.seconds()},
        "limitations": [
            "Labels are synthetic; the model can at best recover the generator's rule from noisy sensor features.",
            "Metrics are from one time-based split of simulated data and are not evidence of operational accuracy.",
            "Live demo data is time-compressed; live predictions use rescaled feature windows (see docs/ml_model.md).",
        ],
    }
    write_json(model_dir / "metadata.json", meta)
    write_json(root / "latest.json", {"version": version, "path": str(model_dir), "trained_at": meta["trained_at"]})
    write_json(reports_dir(cfg) / "ml_evaluation.json", meta)
    pred.unpersist()
    indexed.unpersist()
    return meta


def main() -> None:
    ap = argparse.ArgumentParser(description="Train/evaluate the AEGIS flood-severity Decision Tree (Spark MLlib)")
    ap.parse_args()
    cfg = get_config()
    spark = build_spark("ml-training", cfg)
    meta = train(spark, cfg)
    mt = meta["metrics"]
    print(json.dumps({
        "version": meta["version"], "label_provenance": meta["label_provenance"], "split": meta["split"],
        "accuracy": mt["accuracy"], "weighted_precision": mt["weightedPrecision"],
        "weighted_recall": mt["weightedRecall"], "weighted_f1": mt["weighted_f1"],
        "baseline_majority_class": mt.get("baseline_majority_class"), "per_class": mt["per_class"],
        "confusion_matrix": mt["confusion_matrix"], "feature_importances": meta["tree"]["feature_importances"],
        "reload_check": meta["reload_check"], "durations_s": meta["durations_s"]}, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
