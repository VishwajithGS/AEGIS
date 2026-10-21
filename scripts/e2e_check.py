"""AEGIS end-to-end verification.

Runs every stage against the real services and records each step as PASS,
FAIL, BLOCKED (a prerequisite failed) or SKIPPED, with evidence. Results:
data/reports/e2e_report.json and data/reports/e2e_report.md.

Run inside the aegis-spark container (see scripts/run_e2e.ps1):
    python scripts/e2e_check.py [--live-seconds 60] [--rate 10] [--api-url http://api:8000]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from aegis.config import data_path, get_config  # noqa: E402

PY = sys.executable


class Runner:
    def __init__(self):
        self.steps: list[dict] = []
        self.failed: set[str] = set()

    def step(self, name: str, fn: Callable[[], dict], needs: tuple = (), required: bool = True) -> Optional[dict]:
        blocked = [n for n in needs if n in self.failed]
        rec = {"step": name, "needs": list(needs), "started_at": datetime.now(timezone.utc).isoformat()}
        t0 = time.time()
        if blocked:
            rec.update(status="BLOCKED", evidence={"blocked_by": blocked})
            self.failed.add(name)
        else:
            try:
                ev = fn()
                ok = ev.pop("_ok", True)
                skipped = ev.pop("_skipped", False)
                rec.update(status="SKIPPED" if skipped else ("PASS" if ok else "FAIL"), evidence=ev)
                if not ok:
                    self.failed.add(name)
            except Exception as exc:  # noqa: BLE001 - record any failure as evidence
                rec.update(status="FAIL", evidence={"error": f"{type(exc).__name__}: {exc}",
                                                    "trace": traceback.format_exc()[-1500:]})
                self.failed.add(name)
        rec["seconds"] = round(time.time() - t0, 2)
        self.steps.append(rec)
        print(f"[{rec['status']:7}] {name} ({rec['seconds']} s)")
        if rec["status"] == "FAIL":
            print("          ", json.dumps(rec["evidence"], default=str)[:600])
        return rec.get("evidence")


def sh(args: list[str], timeout: int = 1800) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=os.environ.copy())


def tail(s: str, n: int = 1500) -> str:
    return s[-n:] if s else ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-seconds", type=int, default=60)
    ap.add_argument("--rate", type=float, default=10.0)
    ap.add_argument("--api-url", default=os.environ.get("AEGIS_E2E_API_URL", "http://api:8000"))
    ap.add_argument("--regenerate", action="store_true", help="regenerate the historical backfill")
    args = ap.parse_args()
    cfg = get_config()
    r = Runner()

    # 1. services ---------------------------------------------------------------
    def services():
        from aegis.storage.hbase_repository import HBaseRepository
        from producers.kafka_client import AegisProducer
        topics = AegisProducer(cfg, client_id="aegis-e2e").check_connection(timeout=15)
        hb = HBaseRepository.from_config(cfg).health()
        return {"_ok": hb["available"], "kafka_bootstrap": cfg["kafka"]["bootstrap_servers"],
                "kafka_topics_on_broker": len(topics), "hbase": hb}
    r.step("01 services reachable (Kafka, HBase)", services)

    # 2. topics -----------------------------------------------------------------
    def topics():
        from producers.kafka_admin import create_topics, describe_topics
        res = create_topics(cfg)
        d = describe_topics(cfg)
        return {"_ok": all(v["exists"] for v in d.values()), "create": res,
                "partitions": {k: v.get("partitions") for k, v in d.items()}}
    r.step("02 Kafka topics created/verified", topics, needs=("01 services reachable (Kafka, HBase)",))

    # 3. HBase tables -------------------------------------------------------------
    def hbase_tables():
        p = sh([PY, "hbase/create_tables.py"])
        v = sh([PY, "hbase/verify_hbase.py"])
        out = json.loads(v.stdout) if v.stdout.strip().startswith("{") else {"raw": tail(v.stdout + v.stderr)}
        return {"_ok": p.returncode == 0 and v.returncode == 0, "create_rc": p.returncode,
                "verify_all_passed": out.get("all_passed"), "checks": [(c["check"], c["passed"]) for c in out.get("checks", [])]}
    r.step("03 HBase tables initialised + probe write/read", hbase_tables, needs=("01 services reachable (Kafka, HBase)",))

    # 4. historical data ------------------------------------------------------------
    hist = data_path(cfg, "generated_dir", "historical")

    def generate():
        if args.regenerate or not (hist / "manifest.json").exists():
            p = sh([PY, "-m", "producers.data_generator", "historical"])
            if p.returncode != 0:
                return {"_ok": False, "error": tail(p.stderr)}
        m = json.loads((hist / "manifest.json").read_text())
        return {"line_counts": m["line_counts"], "injected_anomalies": m["injected_anomalies"],
                "label_counts": m["flood_severity_label_counts"], "synthetic": m["synthetic"]}
    r.step("04 synthetic historical data available", generate)

    # 5. publish live events ----------------------------------------------------------
    def publish():
        p = sh([PY, "-m", "producers.run_all", "--rate", str(args.rate), "--duration", str(args.live_seconds)],
               timeout=args.live_seconds + 300)
        st = json.loads((data_path(cfg, "state_dir", "producer_all-sources_status.json")).read_text())
        return {"_ok": p.returncode == 0 and st["delivered"] > 0 and st["failed"] == 0, "sent": st["sent"],
                "delivered": st["delivered"], "failed": st["failed"], "generator_counts": st["generator_counts"]}
    r.step("05 live events published to Kafka", publish, needs=("02 Kafka topics created/verified",))

    # 6. consume ------------------------------------------------------------------
    def consume():
        from producers.kafka_consumer_check import consume as c
        t = cfg["kafka"]["topics"]
        res = c(cfg, [t["weather"], t["water_level"], t["traffic"], t["incidents"]], f"aegis-e2e-{int(time.time())}",
                timeout_s=30, max_messages=500)
        return {"_ok": res["consumed"] > 0, **res}
    r.step("06 Kafka consumer reads the topics", consume, needs=("05 live events published to Kafka",))

    # 7. structured streaming ------------------------------------------------------------
    def streaming():
        from spark_jobs.common import lake
        p = sh([PY, "-m", "spark_jobs.streaming_pipeline", "--available-now", "--require-hbase"], timeout=1800)
        st = json.loads(data_path(cfg, "state_dir", "streaming", "status.json").read_text())
        from producers.kafka_admin import describe_topics
        d = describe_topics(cfg)
        lake_ok = all(Path(lake(cfg, *x)).exists() for x in (("bronze", "kafka_raw"), ("silver_stream", "weather"),
                                                             ("live", "zone_risk"), ("quarantine", "stream")))
        return {"_ok": p.returncode == 0 and lake_ok, "exit_code": p.returncode, "hbase_sink": st.get("hbase_sink"),
                "queries": {k: {"status": v.get("status"), "batch": (v.get("last_progress") or {}).get("batch_id")}
                            for k, v in st.get("queries", {}).items()},
                "last_dq": st.get("last_dq"), "zone_risk_topic_messages": d[cfg["kafka"]["topics"]["zone_risk"]]["messages"],
                "log_tail": tail(p.stdout, 600) if p.returncode else ""}
    r.step("07 Structured Streaming processed Kafka events", streaming,
           needs=("05 live events published to Kafka", "03 HBase tables initialised + probe write/read"))

    # 8. batch ETL ------------------------------------------------------------------
    def etl():
        p = sh([PY, "-m", "spark_jobs.etl_pipeline", "--write-hbase"], timeout=3600)
        rep = json.loads(data_path(cfg, "reports_dir", "dq_report_batch.json").read_text())
        recon = {c["check"]: c["passed"] for c in rep["reconciliation"]}
        return {"_ok": p.returncode == 0 and all(recon.values()) and rep.get("hbase", {}).get("status") == "ok",
                "totals": rep["totals"], "reconciliation": recon, "injected_vs_detected": rep.get("injected_vs_detected"),
                "gold": rep["gold"], "hbase": rep.get("hbase"), "duration_s": rep["duration_s"],
                "stderr_tail": tail(p.stderr, 800) if p.returncode else ""}
    r.step("08 batch ETL + data quality + Parquet/HBase writes", etl, needs=("04 synthetic historical data available",))

    # 9. Parquet read-back ---------------------------------------------------------------
    def parquet():
        import pyarrow.dataset as ds
        from spark_jobs.common import lake
        counts = {}
        for parts in (("silver", "weather"), ("silver", "incidents"), ("gold", "zone_risk_hourly"), ("gold", "zone_hourly"),
                      ("gold", "ml_training_flood_severity"), ("silver_stream", "weather"), ("live", "zone_risk")):
            p = Path(lake(cfg, *parts))
            counts["/".join(parts)] = ds.dataset(str(p), format="parquet", partitioning="hive").count_rows() if p.exists() else 0
        return {"_ok": all(v > 0 for v in counts.values()), "rows_read_back_with_pyarrow": counts}
    r.step("09 Parquet outputs read back", parquet, needs=("08 batch ETL + data quality + Parquet/HBase writes",))

    # 10. RDD + SQL demos ---------------------------------------------------------------
    def demos():
        a = sh([PY, "-m", "spark_jobs.rdd_demo"])
        b = sh([PY, "-m", "spark_jobs.sql_analytics"])
        rdd = json.loads(data_path(cfg, "reports_dir", "rdd_demo.json").read_text())
        sql = json.loads(data_path(cfg, "reports_dir", "sql_analytics.json").read_text())
        return {"_ok": a.returncode == 0 and b.returncode == 0 and rdd["verification_rdd_equals_dataframe"],
                "rdd_category_counts": rdd["incidents_by_category"], "rdd_verified_against_dataframe": rdd["verification_rdd_equals_dataframe"],
                "sql_queries": [(q["name"], q["rows"]) for q in sql["queries"]]}
    r.step("10 Spark RDD and Spark SQL demonstrations", demos, needs=("08 batch ETL + data quality + Parquet/HBase writes",))

    # 11. risk ------------------------------------------------------------------------
    def risk():
        import pyarrow.dataset as ds
        from spark_jobs.common import lake
        out = {}
        for name, parts in (("batch", ("gold", "zone_risk_hourly")), ("live", ("live", "zone_risk"))):
            t = ds.dataset(lake(cfg, *parts), format="parquet", partitioning="hive").to_table(
                columns=["risk_score", "risk_category"]).to_pandas()
            sc = t["risk_score"].dropna()
            out[name] = {"rows": len(t), "min": float(sc.min()), "max": float(sc.max()),
                         "categories": t["risk_category"].value_counts().to_dict()}
        ok = all(0 <= v["min"] and v["max"] <= 100 for v in out.values())
        return {"_ok": ok, **out}
    r.step("11 rule-based zone risk scores within [0,100]", risk,
           needs=("08 batch ETL + data quality + Parquet/HBase writes", "07 Structured Streaming processed Kafka events"))

    # 12. ML training --------------------------------------------------------------------
    def ml():
        p = sh([PY, "-m", "spark_jobs.model_training"])
        meta = json.loads(data_path(cfg, "reports_dir", "ml_evaluation.json").read_text())
        m = meta["metrics"]
        return {"_ok": p.returncode == 0 and meta["reload_check"]["passed"], "version": meta["version"],
                "label_provenance": meta["label_provenance"], "split": meta["split"],
                "accuracy": m["accuracy"], "weighted_f1": m["weighted_f1"], "baseline": m["baseline_majority_class"],
                "confusion_matrix": m["confusion_matrix"], "reload_check": meta["reload_check"]}
    r.step("12 Spark MLlib model trained, evaluated, saved, reloaded", ml,
           needs=("08 batch ETL + data quality + Parquet/HBase writes",))

    # 13. inference -----------------------------------------------------------------------
    def infer():
        rec = {"rain_3h_mm": 60, "rain_max_1h_mm_hr": 35, "level_ratio_max_3h": 1.0, "humidity_avg_3h": 96,
               "disrupted_ratio_1h": 0.6, "speed_ratio_avg_1h": 0.3, "reports_6h": 5, "water_obs_available": 1,
               "traffic_obs_available": 1}
        a = sh([PY, "-m", "spark_jobs.model_inference", "--json", json.dumps(rec)])
        # The live services are paused during the check, so "now" may follow an empty bucket;
        # score as of the end of the newest streamed bucket instead (recorded as features_as_of).
        b = sh([PY, "-m", "spark_jobs.model_inference", "--zones", "--source", "live", "--as-of-latest-event"])
        single = json.loads(a.stdout[a.stdout.index("{"):]) if a.returncode == 0 else {"error": tail(a.stderr)}
        z = json.loads(data_path(cfg, "state_dir", "ml_zone_predictions_latest.json").read_text()) if b.returncode == 0 else {}
        scored = sum(v for k, v in (z.get("distribution") or {}).items() if k != "Insufficient data")
        return {"_ok": a.returncode == 0 and b.returncode == 0 and scored > 0, "zones_with_prediction": scored,
                "features_as_of": z.get("features_as_of"), "single_prediction": single.get("predicted_severity"),
                "probabilities": single.get("probabilities"), "zones_scored": z.get("zones"),
                "zone_distribution": z.get("distribution"), "hbase": z.get("hbase"),
                "stderr_tail": tail(b.stderr, 800) if b.returncode else ""}
    r.step("13 model inference (single record + live zones)", infer,
           needs=("12 Spark MLlib model trained, evaluated, saved, reloaded",))

    # 14. API -----------------------------------------------------------------------------
    def api():
        import httpx
        base = args.api_url.rstrip("/")
        out, ok = {}, True
        with httpx.Client(timeout=60) as c:
            for path in ("/health", "/api/overview", "/api/zones", "/api/zones/Z-R04-C02", "/api/incidents?limit=5",
                         "/api/incidents/nearby?lat=13.05&lon=80.22&radius_km=3", "/api/analytics/historical?days=14",
                         "/api/analytics/data-quality", "/api/pipeline/status", "/api/model/status", "/api/alerts"):
                resp = c.get(base + path)
                out[path] = resp.status_code
                ok &= resp.status_code == 200
            for _ in range(30):                       # the API loads the model in the background
                if c.get(base + "/api/model/status").json().get("state") in ("ready", "error", "unavailable"):
                    break
                time.sleep(2)
            pr = c.post(base + "/api/model/predict", json={"rain_3h_mm": 50, "rain_max_1h_mm_hr": 30, "reports_6h": 3,
                                                           "water_obs_available": 0, "traffic_obs_available": 0})
            out["POST /api/model/predict"] = pr.status_code
            out["prediction"] = pr.json().get("predicted_severity") if pr.status_code == 200 else pr.text[:200]
            ok &= pr.status_code == 200
            z = c.get(base + "/api/zones").json()
            out["zone_origins"] = {k: sum(1 for x in z["zones"] if (x["assessment"] or {}).get("data_origin") == k)
                                   for k in ("live-stream", "batch-historical")}
        return {"_ok": ok, "api_url": base, "responses": out}
    r.step("14 FastAPI endpoints respond with project data", api,
           needs=("08 batch ETL + data quality + Parquet/HBase writes",))

    summary = {s: sum(1 for x in r.steps if x["status"] == s) for s in ("PASS", "FAIL", "BLOCKED", "SKIPPED")}
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "summary": summary, "steps": r.steps,
              "note": "All data is synthetic. Each status reflects an executed check; see evidence."}
    out_dir = data_path(cfg, "reports_dir")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "e2e_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    lines = ["# AEGIS end-to-end report", "", f"Generated: {report['generated_at']}", "",
             f"**Summary:** {summary}", "", "| # | Step | Status | Seconds |", "|---|---|---|---|"]
    lines += [f"| {i + 1} | {s['step'][3:]} | {s['status']} | {s['seconds']} |" for i, s in enumerate(r.steps)]
    (out_dir / "e2e_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary))
    return 0 if summary["FAIL"] == 0 and summary["BLOCKED"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
