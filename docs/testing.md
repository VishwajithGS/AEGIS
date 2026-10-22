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
`test_api.py` FastAPI with a temporary lake and HBase/Kafka deliberately unreachable: batch fallback + `data_origin`/`stale` flags, sorting/filtering, 404/422 validation, Haversine nearby search excluding rejected reports, 503 when no model, degraded pipeline status ·
`test_realdata.py` (real-data adapters, offline, using real excerpts in `tests/fixtures/real/`) NetCDF-3 reader round trip and fill values, IMD nearest *land* cell, ERA5 parsing (Michaung 48 h sum 272.9 mm) and rejection of wrong units/time zone/missing fields, IMD intensity classes at every threshold, GHCN-D tenths-of-mm conversion (249.9 mm on 2023-12-04) with gaps kept missing, DFO and GDACS parsing, deterministic ids, HTTP cache and SHA-256 with a local server, future end dates refused ·
`test_hybrid.py` hybrid generator: the rain field equals the ERA5 value of the zone and hour (0 where ERA5 has none), every synthetic line is tagged `aegis-simulator-era5-driven`, real files are copied byte-for-byte, the station registry merges ERA5 stations with synthetic gauges/segments, output is deterministic, non-ERA5 weather input is refused, the API labels the mode ·
`test_parquet_store.py` lake reader: row counts and reads equal pyarrow directory discovery (Hive partition columns, ignored `_temporary` / dot files), new and removed files are picked up ·
`test_api_realmode.py` API on a real-data root: `/api/dataset` and overview report `real-historical`, real-data analytics with empty incident tables, no false "unreachable" issues for services that are deliberately off.

**Spark:**
`test_transforms.py` both weather vendor formats → canonical units and UTC; cm → m and level ratio; local Asia/Kolkata time → UTC; category/verification normalisation; 15 rejection cases each producing its reason code; late-arrival flag; Spark/Python zone-id parity on 409 points incl. cell boundaries; Spark/Python Haversine parity; de-duplication keeps the first arrival ·
`test_pipeline_jobs.py` batch ETL on a 3-day generated set: all reconciliation checks pass and **every injected defect is detected with the expected reason** (counts equal); silver/gold/quarantine/bronze readable; risk within [0, 100]; ML features exclude the label; RDD demo uses all required transformations/actions and matches the DataFrame result; every Spark SQL query runs; risk UDF equals the Python reference; zone integration with missing domains; Kafka record counts from offset ranges; restart back-off schedule ·
`test_realdata_pipeline.py` real ERA5 records → canonical weather with `data_source`/`observation_type`; synthetic records tagged `aegis-simulator`; without a report source every zone-hour is "Insufficient data" (and synthetic behaviour unchanged); hazard index NULL without an incident archive; batch ETL + real-data layers in real mode (no `passed = false` check, absent domains readable, 26 events, IMD/ERA5 pairs); batch ETL in hybrid mode (zones scored instead of "Insufficient data", ML check applies, silver `data_source` real for weather and `aegis-simulator-era5-driven` for the rest) ·
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

After the real-data migration (2026-10-22):

| Suite | Command | Result |
|---|---|---|
| Host unit tests | `.\.venv\Scripts\python.exe -m pytest tests/unit -q` | **90 passed** (55 + 25 real-data + 5 hybrid + 3 real-mode API + 2 lake reader) |
| Container: unit + Spark + integration | `docker compose run --rm tools python -m pytest tests -q -rs` | **138 passed** (90 unit, 43 Spark, 5 integration), 0 skipped. An earlier run failed 1 test on an outdated 14-event fixture; the fixture was refreshed from the import (26 events) |
| Hybrid dashboard | `.\.venv\Scripts\python.exe scripts\ui_check.py --hybrid --url http://127.0.0.1:5175 --out data_hybrid/reports/ui` | 7/7 tabs, 0 console errors, 0 failed requests |
| End-to-end | `scripts\run_e2e.ps1` | first run 13/14 (step 14 API timeout on ~10,000 streamed files, fixed in `ParquetStore`), re-run **14/14 PASS** |
| Synthetic dashboard | `.\.venv\Scripts\python.exe scripts\ui_check.py` | 7/7 tabs, 0 console errors, 0 failed requests (one run right after the container suite reported a tab that did not render within its 45 s wait; the next two runs passed 7/7) |
| Real-mode dashboard | `.\.venv\Scripts\python.exe scripts\ui_check.py --real --url http://127.0.0.1:5174 --out data_real/reports/ui` | 7/7 tabs, 0 console errors, 0 failed requests |

Mocks are not used for HBase or Kafka anywhere; tests either talk to the live
service or skip with the reason (there were no skips in the runs above).
