"""Spark MLlib model hosting for the API.

The model is optional: if PySpark/Java are unavailable in this process, or
no model has been trained, the status says so and prediction requests get
HTTP 503 - the rest of the API is unaffected. Model metadata (metrics,
provenance) is read from disk and shown even when Spark is unavailable.
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from aegis.config import data_path
from aegis.logging_config import setup_logging
from aegis.ml_contract import FEATURE_COLUMNS, FEATURE_DOCS, validate_features
from aegis.storage.hbase_repository import HBaseUnavailable, prediction_row_key
from backend.app.services.hbase_service import HBaseService
from backend.app.services.parquet_store import read_json

log = setup_logging("api.model")


class ModelService:
    def __init__(self, cfg: dict, hbase: HBaseService, enabled: bool = True):
        self.cfg, self.hbase, self.enabled = cfg, hbase, enabled
        self.state, self.reason = "not_loaded", None
        self.model = None
        self.spark = None
        self._lock = threading.Lock()
        self.root = data_path(cfg, "models_dir", cfg["model"]["name"])

    # -- metadata --------------------------------------------------------------
    def latest_version(self) -> Optional[str]:
        p = read_json(self.root / "latest.json")
        return p.get("version") if p else None

    def metadata(self, version: Optional[str] = None) -> Optional[dict]:
        version = version or self.latest_version()
        return read_json(self.root / version / "metadata.json") if version else None

    # -- loading ---------------------------------------------------------------
    def start(self) -> None:
        if not self.enabled:
            self.state, self.reason = "unavailable", "model hosting disabled (AEGIS_API_MODEL=0)"
            return
        threading.Thread(target=self._load, name="model-loader", daemon=True).start()

    def _load(self) -> None:
        with self._lock:
            version = self.latest_version()
            if version is None:
                self.state, self.reason = "not_trained", "no trained model (run scripts/train_model.ps1)"
                return
            self.state = "loading"
            try:
                from spark_jobs.common import build_spark            # imports pyspark
                from spark_jobs.model_inference import SeverityModel
            except ImportError as exc:
                self.state, self.reason = "unavailable", f"Spark runtime not available in this API process: {exc}"
                return
            try:
                t0 = time.time()
                if self.spark is None:
                    self.spark = build_spark("api-model", self.cfg, master="local[1]", shuffle_partitions=1,
                                             driver_memory="768m")
                self.model = SeverityModel(self.spark, self.root / version)
                self.state, self.reason = "ready", None
                log.info(f"model {version} loaded in {time.time() - t0:.1f}s")
            except Exception as exc:  # noqa: BLE001 - JVM/model errors are reported via status
                self.state, self.reason = "error", f"{type(exc).__name__}: {exc}"
                log.error(f"model load failed: {self.reason}")

    def _maybe_reload(self) -> None:
        latest = self.latest_version()
        if self.state == "ready" and self.model is not None and latest and latest != self.model.version:
            log.info(f"new model version {latest} detected; reloading")
            threading.Thread(target=self._load, daemon=True).start()
        elif self.state == "not_trained" and latest:
            threading.Thread(target=self._load, daemon=True).start()

    # -- API -------------------------------------------------------------------
    def status(self) -> dict:
        self._maybe_reload()
        meta = self.metadata(self.model.version if self.model else None)
        out = {"state": self.state, "reason": self.reason, "available": self.state == "ready",
               "kind": "ML-predicted flood-incident severity (supporting component; independent of rule-based risk)",
               "features": [{"name": f, "unit": FEATURE_DOCS[f][0], "description": FEATURE_DOCS[f][1]}
                            for f in FEATURE_COLUMNS]}
        if meta:
            m = meta["metrics"]
            out.update({
                "version": meta["version"], "trained_at": meta["trained_at"], "algorithm": meta["algorithm"],
                "label_provenance": meta["label_provenance"], "labels_are_synthetic": "SYNTHETIC" in meta["label_provenance"],
                "labels": meta["labels"], "split": meta["split"], "params": meta["params"],
                "evaluation": {k: m[k] for k in ("accuracy", "weightedPrecision", "weightedRecall", "weighted_f1",
                                                 "per_class", "confusion_matrix", "baseline_majority_class", "test_rows")
                               if k in m},
                "feature_importances": meta["tree"]["feature_importances"], "tree_depth": meta["tree"]["depth"],
                "limitations": meta.get("limitations", []), "reload_check": meta.get("reload_check"),
            })
        zone_preds = read_json(data_path(self.cfg, "state_dir", "ml_zone_predictions_latest.json"))
        if zone_preds:
            out["zone_predictions"] = {k: zone_preds[k] for k in ("model_version", "source", "features_as_of",
                                                                  "predicted_at", "distribution") if k in zone_preds}
        return out

    def predict(self, record: dict, persist: bool = True) -> dict:
        if self.state != "ready" or self.model is None:
            raise RuntimeError(self.reason or f"model state is {self.state}")
        validate_features(record, self.model.features)         # raises FeatureValidationError
        res = self.model.predict_records([record])[0]
        res["prediction_id"] = uuid.uuid4().hex[:16]
        res["zone_id"] = record.get("zone_id")
        res["input_source"] = record.get("input_source") or "api request"
        res["persisted"] = {"hbase": False}
        if persist:
            ts = int(time.time() * 1000)
            key = prediction_row_key(res["zone_id"] or "api", ts, res["prediction_id"])
            try:
                ok = self.hbase.put_prediction(key, {
                    "input": {**res["features"], "input_source": res["input_source"]},
                    "output": {"predicted_severity": res["predicted_severity"], "probabilities": res["probabilities"]},
                    "meta": {"prediction_id": res["prediction_id"], "zone_id": res["zone_id"],
                             "model_version": res["model_version"], "label_provenance": res["label_provenance"],
                             "predicted_at_iso": res["predicted_at"]}})
                res["persisted"] = {"hbase": ok, "table": "model_predictions", "row_key": key}
            except HBaseUnavailable as exc:
                res["persisted"] = {"hbase": False, "error": str(exc)}
        return res
