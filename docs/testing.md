# Testing AEGIS

| Suite | Location | Needs | Command (PowerShell, project root) |
|---|---|---|---|
| Unit | `tests/unit` | Python only (host `.venv` or container) | `.\.venv\Scripts\python.exe -m pytest tests/unit -q` |
| Spark | `tests/spark` (marker `spark`) | PySpark 3.5 + Java 17 (container) | `docker compose run --rm --no-deps tools python -m pytest tests/spark -q` |
| Integration | `tests/integration` (marker `integration`) | live Kafka + HBase (+ Spark for streaming) | `docker compose run --rm tools python -m pytest tests/integration -q -rs` |
| End-to-end | `scripts/e2e_check.py` | full stack | `scripts\run_e2e.ps1` |
| All | | | `scripts\run_tests.ps1` |

Spark fixtures skip with an explicit reason when PySpark or a Java 11/17/21
runtime is missing; integration tests skip with the unreachable endpoint
(`Kafka not reachable at localhost:9092`). A skip is never reported as a pass:
`-rs` prints every skip reason.

## What is covered

**Unit (no services):**
`test_geo.py` coordinate validation, Haversine (exact value on the equator, symmetry, vectorised form), grid edges/clamping, zone ids, nearest-station radius ·
`test_risk.py` normalisation, categories, confidence, contributions sum to the score, *missing data does not mean safe*, insufficient data → no score, NaN handling, bounds, configurable weights ·
`test_config.py` typed env overrides, invalid weights/ramps rejected, secret redaction ·
`test_hbase_keys.py` row-key formats (incl. separate live / hourly zone-risk series), fixed-width ordering, reverse timestamps, sparse cell encoding round trip, table specs ·
`test_ml_contract.py` feature validation (required, bounds, binary flags, NaN, unknown fields such as `severity`) ·
`test_generator.py` byte-identical deterministic output, synthetic marking, every anomaly kind, label rule thresholds, wall-clock stamping of live events, report jitter never leaves the study region ·
`test_api.py` FastAPI with a temporary lake and HBase/Kafka deliberately unreachable: batch fallback + `data_origin`/`stale` flags, sorting/filtering, 404/422 validation, Haversine nearby search excluding rejected reports, 503 when no model, degraded pipeline status.

**Spark:**
`test_transforms.py` both weather vendor formats → canonical units and UTC; cm → m and level ratio; local Asia/Kolkata time → UTC; category/verification normalisation; 15 rejection cases each producing its reason code; late-arrival flag; Spark/Python zone-id parity on 409 points incl. cell boundaries; Spark/Python Haversine parity; de-duplication keeps the first arrival ·
`test_pipeline_jobs.py` batch ETL on a 3-day generated set: all reconciliation checks pass and **every injected defect is detected with the expected reason** (counts equal); silver/gold/quarantine/bronze readable; risk within [0, 100]; ML features exclude the label; RDD demo uses all required transformations/actions and matches the DataFrame result; every Spark SQL query runs; risk UDF equals the Python reference; zone integration with missing domains; Kafka record counts from offset ranges; restart back-off schedule ·
`test_model.py` metrics in range, confusion matrix sums to the test size, beats the majority baseline, time split has no overlap, save → reload → identical predictions, prediction schema/labels/probabilities, imputed-feature reporting, leakage/missing-column/size/label validation, missing model → `ModelUnavailable`.

**Integration:**
`test_kafka_live.py` temporary topic produce → consume (25 messages, zero failures) and AEGIS topic layout ·
`test_hbase_live.py` batched puts, get, prefix count, reverse scan, three cell versions, deletes; clear error on an unreachable server ·
`test_streaming_live.py` isolated topics + temporary lake: valid, duplicate, malformed and out-of-range events through the real streaming job (`--available-now`); asserts de-duplicated silver, quarantine reasons, bronze count, live zone risk in range, HBase sensor row with matching values, zone-risk and DQ topics populated.

## UI check

`scripts/ui_check.py` drives the installed Edge (or Chrome) through Playwright
(no browser download), opens every dashboard tab, waits for project data,
saves screenshots to `data/reports/ui/` and fails on browser console errors or
failed HTTP requests. Install once:
`.\.venv\Scripts\python.exe -m pip install playwright==1.55.0`.

## Verification status (2026-10-21, project machine)

| Suite | Command | Result |
|---|---|---|
| Host unit tests | `.\.venv\Scripts\python.exe -m pytest tests/unit -q` | **55 passed** |
| Container: unit + Spark + integration | `docker compose run --rm tools python -m pytest tests -q -rs` | **97 passed** (55 unit, 37 Spark, 5 integration), 0 skipped |
| Script | `scripts\run_tests.ps1` | all suites passed, exit 0 |
| End-to-end | `scripts\run_e2e.ps1` | **14/14 PASS** (details in `docs/traceability.md`, "End-to-end") |
| Dashboard | `.\.venv\Scripts\python.exe scripts\ui_check.py` | 7/7 tabs, 0 console errors, 0 failed requests |

Mocks are not used for HBase or Kafka anywhere; tests either talk to the live
service or skip with the reason (there were no skips in the runs above).
