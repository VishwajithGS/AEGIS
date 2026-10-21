"""Live HBase verification: write a probe row to every AEGIS table, read it
back, check cell versions, scan by prefix, count pipeline rows, then delete
the probes. Exit code 0 only if every read-back matches what was written.

    python hbase/verify_hbase.py
"""
from __future__ import annotations

import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aegis.config import get_config  # noqa: E402
from aegis.storage.hbase_repository import (TABLES, HBaseRepository, HBaseUnavailable, metric_row_key,  # noqa: E402
                                            prediction_row_key, sensor_row_key, zone_risk_row_key)


def main() -> int:
    cfg = get_config()
    repo = HBaseRepository.from_config(cfg)
    checks, now = [], int(time.time() * 1000)
    tag = uuid.uuid4().hex[:8]
    probes = {
        "sensor_observations": (sensor_row_key("VERIFY-PROBE", now, f"probe-{tag}"),
                                {"measurement": {"rainfall_mm_hr": 12.5}, "location": {"zone_id": "Z-R00-C00"},
                                 "metadata": {"event_id": f"probe-{tag}", "is_synthetic": True}}),
        "incident_records": (f"VERIFY-PROBE-{tag}",
                             {"incident": {"category": "flood"}, "location": {"lat": 13.0},
                              "verification": {"verification_status": "unverified"}}),
        "zone_risk_history": (zone_risk_row_key("VERIFY-PROBE", now),
                              {"risk": {"risk_score": 10.0, "risk_category": "Low"}, "summary": {"reports": 0},
                               "quality": {"weather_obs": 1}}),
        "processing_metrics": (metric_row_key("verify.probe", now),
                               {"metrics": {"records": 1}, "status": {"state": "ok"}}),
        "model_predictions": (prediction_row_key("VERIFY-PROBE", now, tag),
                              {"input": {"rain_3h_mm": 1.0}, "output": {"predicted_severity": "Low"},
                               "meta": {"model_version": "probe"}}),
    }
    try:
        health = repo.health()
        if not health["available"]:
            raise HBaseUnavailable(health["error"])
        repo.ensure_tables()
        for table, (key, fams) in probes.items():
            repo.put(table, key, fams)
            got = repo.get(table, key)
            ok = got is not None and all(str(got.get(f, {}).get(q)).lower() == str(v).lower()
                                         for f, cols in fams.items() for q, v in cols.items())
            checks.append({"check": f"{table}: put then get returns identical cells", "passed": ok,
                           "row_key": key, "read_back": got})
        # multi-version cells on zone_risk_history.risk (VERSIONS=5)
        key = probes["zone_risk_history"][0]
        repo.put("zone_risk_history", key, {"risk": {"risk_score": 55.0, "risk_category": "High"}})
        versions = repo.cell_versions("zone_risk_history", key, "risk:risk_score", 5)
        checks.append({"check": "zone_risk_history keeps previous versions of risk:risk_score",
                       "passed": [v["value"] for v in versions][:2] == [55.0, 10.0], "versions": versions})
        latest = repo.latest_with_prefix("zone_risk_history", "VERIFY-PROBE#")
        checks.append({"check": "reverse prefix scan returns the probe row", "passed": latest is not None
                       and latest[0] == key})
        counts = {t: repo.count_rows(t, max_rows=2_000_000) for t in TABLES}
        for table, (key, _) in probes.items():
            repo.delete(table, key)
        gone = all(repo.get(t, k) is None for t, (k, _) in probes.items())
        checks.append({"check": "probe rows deleted", "passed": gone})
        out = {"endpoint": health["endpoint"], "tables": health["tables"], "row_counts_including_probes": counts,
               "checks": checks, "all_passed": all(c["passed"] for c in checks)}
        print(json.dumps(out, indent=2, default=str))
        return 0 if out["all_passed"] else 1
    except HBaseUnavailable as exc:
        print(json.dumps({"all_passed": False, "error": str(exc),
                          "hint": "HBase is not reachable; start it with: docker compose up -d hbase"}, indent=2))
        return 2
    finally:
        repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
