# Faculty requirement traceability

Source of requirements: *Big Data and Modern Databases — Final Package*
(`Lab Package_Instructions.docx.pdf`): semi-structured, streaming and
time-series data; HBase; Apache Spark and Kafka; Python; ETL, data mapping and
integration; standardisation, sorting, aggregation, validation, verification;
an ML model / data analytics; Spark RDD transformations & actions, Spark SQL
& ML libraries, real-time streaming, wide-column data model; reference
architecture (ingest → stream processing → cold/analytical storage → ML,
monitoring, dashboards).

**Status legend:** VERIFIED = executed on the project machine with the evidence
shown (2026-10-21, Docker Desktop 29.8.0, images `aegis-spark:3.5.9` and
`aegis-hbase:2.5.16`, 60-day synthetic backfill seed 42). Nothing below is
estimated; every number comes from a run whose command is listed.

| # | Requirement | Implementation | Demonstration command | Test | Evidence (actual run) | Status |
|---|---|---|---|---|---|---|
| 1 | Python | `aegis/`, `producers/`, `spark_jobs/`, `backend/`, `hbase/`, `scripts/` | any script | whole suite | 97/97 tests pass in the container; 55/55 host unit tests (2026-10-21). After the real-data migration and hybrid mode (2026-10-22): 138/138 container tests, 90/90 host unit tests | VERIFIED |
| 2 | Semi-structured data | JSON events, 6 schema versions, nested fields (`producers/formats.py`) | `Get-Content data\generated\historical\weather.jsonl -TotalCount 3` | `test_transforms.py` | both weather vendor formats (°F, in/hr, epoch ms, nested location) map to one canonical schema | VERIFIED |
| 3 | Streaming data | Kafka producers + Structured Streaming | `scripts\start_producers.ps1`, `scripts\start_streaming.ps1` | `test_streaming_live.py` | continuous run at 4 events/s for >30 min; sent 11,713 / delivered 11,712 (1 in flight) / failed 0 | VERIFIED |
| 4 | Time-series data | UTC event time from 3 timestamp styles, 5-min stream windows, hourly/daily gold tables, LAG trends, moving averages | `scripts\run_batch_analytics.ps1 -Only sql` | `test_transforms.py`, `test_pipeline_jobs.py` | q3 6-hour windows, q4 LAG trend, q7 3-day moving average returned rows; 56,346 zone-hours | VERIFIED |
| 5 | Kafka | `producers/kafka_client.py`, `kafka_admin.py`, `kafka_consumer_check.py`, Kafka 4.3.1 KRaft | `python -m producers.kafka_admin describe`, `python -m producers.kafka_consumer_check` | `test_kafka_live.py` | 6 topics × 3 partitions; 183 sent = 183 delivered = 183 consumed (host run) | VERIFIED |
| 6 | Apache Spark | `spark_jobs/` (PySpark 3.5.9, Java 17) | `scripts\run_batch_analytics.ps1` | `tests/spark` (37) | all batch jobs exit 0 | VERIFIED |
| 7 | HBase wide-column | 5 tables, `aegis/storage/hbase_repository.py`, `spark_jobs/hbase_sink.py` | `python hbase/verify_hbase.py` | `test_hbase_live.py`, `test_hbase_keys.py` | all 8 live checks pass (put/get, 5-version cells, reverse prefix scan, delete); ETL loaded 7,013 incident / 16,749 sensor / 56,381 zone-risk rows, samples read back | VERIFIED |
| 8 | RDD transformations/actions | `spark_jobs/rdd_demo.py` | `scripts\run_batch_analytics.ps1 -Only rdd` | `test_rdd_demo_runs_and_matches_dataframe` | 13 operations (textFile, map, filter, flatMap, reduceByKey, aggregateByKey, mapValues, distinct, sortBy; count, first, take, collect, reduce), lineage printed, RDD counts = DataFrame counts | VERIFIED |
| 9 | Spark SQL | `spark_jobs/sql_analytics.py` (10 queries) | `scripts\run_batch_analytics.ps1 -Only sql` | `test_sql_queries_execute` | all 10 queries return rows (joins, CTEs, window(), LAG, RANK, moving average, LATERAL VIEW explode) | VERIFIED |
| 10 | Structured Streaming | 3 queries, 10-min watermark, `dropDuplicatesWithinWatermark`, update-mode windows, checkpoints, listener | `scripts\start_streaming.ps1` | `test_streaming_live.py` | dedup, quarantine, bronze count and HBase row asserted; late events dropped by watermark reported (e.g. 8); resume from checkpoint after restart; graceful stop 37 s exit 0; recovery after a 75 s HBase outage | VERIFIED |
| 11 | Spark ML library | `model_training.py`, `model_inference.py` (DecisionTree pipeline) | `scripts\train_model.ps1 -Infer` | `test_model.py` | accuracy 0.677 vs 0.323 majority baseline, weighted F1 0.664, reload check passed, 39/40 zones predicted from live data (labels synthetic) | VERIFIED |
| 12 | ETL | `etl_pipeline.py` bronze → silver → gold | `scripts\run_batch_analytics.ps1 -Only etl` | `test_pipeline_jobs.py` | 296,817 backfill records (+ streamed records) processed; all 9 reconciliation checks pass | VERIFIED |
| 13 | Data mapping & integration | `transforms.py` source → canonical; `risk_analytics.zone_signals` joins 4 sources per zone | ETL | `test_transforms.py`, `test_zone_signals_with_missing_domains` | unit (cm/ft → m, °F → °C, in → mm), time (+05:30, Z, epoch ms, naive local) and category mappings verified | VERIFIED |
| 14 | Standardisation, sorting, aggregation, validation, verification | canonical units/categories/UTC; sortBy / ORDER BY / RANK; window and zone aggregates; 12 rejection codes; reconciliation | ETL, RDD, SQL | `test_transforms.py` (15 rejection cases), `test_pipeline_jobs.py` | injected = detected for all 8 defect kinds (e.g. malformed_json 640/640, outside_study_region 355/355, duplicates 1,944/1,944) | VERIFIED |
| 15 | High-volume, multi-source design | 4 topics × 3 partitions, 6 formats, partitioned Parquet, per-partition HBase writers | generator + ETL | — | 296,817 records in 126–220 s batch ETL; streaming keeps up at 4 events/s with stable batch sizes. Single-node only: no distributed scalability measured | VERIFIED (single node) |
| 16 | Meaningful analytics | rule-based zone risk with contributions and confidence, Haversine spatial analysis, correlations, trends | `scripts\run_batch_analytics.ps1` | `test_risk.py`, `test_geo.py`, `test_risk_udf_matches_python_reference` | risk 0–96.99 batch / 0–80.69 live; corr(rain, traffic disruption) 0.657; 19/40 zones with a gauge in range (others report water level as not observed) | VERIFIED |
| 17 | End-to-end demonstration | `scripts/e2e_check.py`, `scripts\run_e2e.ps1`, `docs/demo_guide.md` | `scripts\run_e2e.ps1` | — | see "End-to-end" below | VERIFIED |
| 18 | Setup, testing, documentation | README, `docs/*`, 12 PowerShell entry-point scripts | `scripts\setup.ps1 -HostVenv` … `scripts\stop_all.ps1` | `scripts\run_tests.ps1` | every script executed (see below) | VERIFIED |
| — | Parquet | partitioned lake (`data/lake`) | e2e step 09 | `test_etl_outputs_readable` | 7 tables read back with pyarrow | VERIFIED |
| — | Dashboard | `frontend/` React + Vite + Leaflet | `scripts\start_frontend.ps1` | `scripts\ui_check.py` | all 7 tabs render project data in Edge; 0 console errors, 0 failed requests; screenshots in `data\reports\ui\` | VERIFIED |
| — | API | `backend/app` FastAPI | `scripts\start_api.ps1` → `/docs` | `test_api.py`, e2e step 14 | all endpoints 200 with live data; 404/422/503 paths; predictions persisted to HBase and read back | VERIFIED |

## End-to-end

`scripts\run_e2e.ps1` (live services paused and restored by the script), final
run 2026-10-21 15:18 UTC on the final code: **14 PASS, 0 FAIL, 0 BLOCKED, 0 SKIPPED**.

| Step | Evidence |
|---|---|
| 05 publish | 60 s at 10 events/s; sent = delivered, 0 failed |
| 07 Structured Streaming | exit 0, HBase sink enabled; last micro-batch 518 records → 498 valid, 20 rejected |
| 08 batch ETL | 316,208 records (backfill + archived stream); 9/9 checks; HBase 7,104 incident / 22,556 sensor / 56,420 zone-risk rows |
| 09 Parquet | 7 tables read back with pyarrow |
| 11 risk range | batch and live scores within [0, 100] |
| 12 ML | trained, evaluated, saved, reloaded (accuracy 0.677) |
| 13 inference | single record → High; 39/40 zones predicted from live data (Low 17 · Moderate 12 · High 10 · 1 insufficient data) |
| 14 API | every endpoint 200; 39 zones served as `live-stream` from HBase |

Re-run after the real-data migration, 2026-10-22 13:28 UTC: first attempt
13 PASS, 1 FAIL (step 14 `ReadTimeout`, fixed — see "Problems found" below);
second attempt **14 PASS, 0 FAIL, 0 BLOCKED, 0 SKIPPED**. Step 08: 411,430
records (backfill + ~1 day of archived stream), 9/9 checks, every injected
defect detected. Step 12: accuracy 0.660, weighted F1 0.643 — lower than
0.677 because the backfill had been regenerated on 2026-10-21 16:35 UTC and
its last hours now overlap archived live-stream records, which change the
features of the newest incidents. On the backfill alone
(`etl_pipeline --no-stream-bronze` + `model_training`) the model reproduces
accuracy 0.6767, weighted F1 0.6637 and the per-class scores exactly, so the
migration did not change the synthetic results. Step 14: all endpoints 200 in
18 s.

## Scripts executed

All 12 PowerShell entry-point scripts (plus the shared `common.ps1`) were run as documented and exited 0: `setup.ps1 -HostVenv`,
`start_infrastructure.ps1`, `generate_data.ps1`, `run_batch_analytics.ps1`
(all and `-Only etl`), `train_model.ps1 -Infer`, `start_streaming.ps1`,
`start_producers.ps1`, `start_api.ps1`, `start_frontend.ps1`, `run_tests.ps1`,
`run_e2e.ps1`, `stop_all.ps1` (graceful: queries stopped after in-flight
batches, status `stopped`, exit 0), plus `scripts\ui_check.py`. A cold start
after `stop_all.ps1` (infrastructure → streaming → producers → API → dashboard)
reached pipeline status `ok` with no issues.

## Problems found by running it, and fixed

| Problem (as observed) | Fix |
|---|---|
| Image build: PyPI read timeout on the 318 MB PySpark archive | pip timeout/retries; optional checksum-verified local archive (`docker/spark/wheels/`) |
| 24 citizen reports near the region edge rejected as `outside_study_region` but not counted as injected (24 extra) | report jitter clamped inside the region; regression test |
| Live zone inference crashed (`report_weight` unresolved) when no live reports existed yet | missing domain columns added as typed NULLs; regression test |
| Empty live window → imputed "rainy" medians → every zone "Moderate" | zones without weather observations get "Insufficient data" |
| Streaming JVM OOM-killed at the 3 GB container limit (`local[*]` = 16 Python workers) | `local[4]`, 1 GB heap; crash recorded in status; auto-restart |
| SIGTERM stop exceeded 90 s and was killed (exit 137) | concurrent stop after in-flight batch (37 s, exit 0) |
| Dashboard input rates overstated 4–16× | Kafka records from offset ranges |
| Batch hourly and live 5-min zone-risk rows overwrote each other in HBase | `zone_id#live#…` / `zone_id#hourly#…` key series |
| HBase aborted after a 42 s host pause (memory pressure; 40 s ZooKeeper session cap) | Spark resource caps, 120 s ZooKeeper sessions, auto-restart; connect retries; ETL exits 1 if the requested HBase load fails |
| One streaming outage could exhaust restart attempts | back-off and counter reset |
| `run_e2e.ps1` would share checkpoints with a running streaming service | script pauses/restores `streaming` and `producers` |
| 2026-10-22: e2e step 14 FAIL (`ReadTimeout`) after ~1 day of streaming: `/api/overview` 75 s, `/api/pipeline/status` 82 s. pyarrow directory discovery stats every file (~5 ms each on the Windows bind mount) and ~10,000 micro-batch files had accumulated | `ParquetStore` lists files with `os.walk`, builds the dataset from that list (pyarrow's own Hive inference, same schema), reads fragments in parallel and remembers per-file row counts: overview 19 s cold / 8 s warm, pipeline status 4 s / 1.4 s; regression tests `tests/unit/test_parquet_store.py` |
| 2026-10-22: hybrid model training crashed (`NoSuchElementException: key not found: 3.0`) — the time-split test set had no Critical incident and Spark's per-label evaluator fails for an absent class | per-class scores computed from the confusion matrix with Spark's formulas (verified identical on the synthetic model: all 12 values equal); absent classes reported as `null` with support 0 |

## Real-data migration (2026-10-22)

Requirements of the dataset-audit brief and where each is met. Details and
numbers: [dataset_audit.md](dataset_audit.md),
[dataset_migration_plan.md](dataset_migration_plan.md).

| Requirement | Implementation | Command | Test | Evidence (actual run) | Status |
|---|---|---|---|---|---|
| Audit every synthetic dataset | `docs/dataset_audit.md` (15 datasets/artefacts) | — | — | every generator output, fixture and derived artefact listed with its consumers | DONE |
| Verify real sources | `docs/dataset_migration_plan.md` §1 | requests listed per row | — | 19 candidates checked from this machine: 5 used (ERA5, IMD, GHCN-D, DFO, GDACS); 2 verified but not implemented (SACHET, GloFAS); 3 verified but rejected or cross-check only (ISD-Lite, Meteostat, NASA POWER); 4 blocked by accounts/keys (IMERG, Global Flood Database, ReliefWeb, data.gov.in); 2 unreachable (India-WRIS, mausam.imd.gov.in); CWC without an open API; no open source for traffic or citizen reports | VERIFIED |
| Mapping with feasibility classes | plan §2 (D/C/P/X/N) | — | — | rainfall/temperature/humidity **C**; daily rainfall and events **X**; water level **P/X**; traffic, reports, severity labels **N** | DONE |
| Ingestion layer: configurable, idempotent, provenance | `aegis/realdata/` (`common`, `era5`, `daily_rainfall`, `events`, `netcdf3`, `ingest`) | `scripts\ingest_real_data.ps1` | `tests/unit/test_realdata.py` (25) | 57,600 + 2,460 + 26 records; SHA-256 per response; re-run byte-identical; failed download exits 2, nothing substituted | VERIFIED |
| Existing pipeline unchanged in contract | ERA5 as `wx-era5-1` in the weather family; `data_source` / `observation_type` columns added | `scripts\run_real_data.ps1` | `tests/spark/test_realdata_pipeline.py` (5) | ETL 57,600 valid / 0 rejected; checks PASS or not applicable | VERIFIED |
| Missing sources not treated as safe | `risk.reports_observed=false`; hazard index NULL without archive | as above | `test_missing_report_source_is_not_observed`, `test_hazard_index_is_null_without_incident_archive` | 57,600/57,600 zone-hours "Insufficient data" | VERIFIED |
| Real and synthetic never mixed | separate `data_real/` root, manifest `data_mode`, real-mode API with HBase / live feed / model off | `docker compose --profile real up -d api-real` | `tests/unit/test_api_realmode.py` (3) | `/api/dataset` = `real-historical`; pipeline status `ok` without false "unreachable" issues | VERIFIED |
| Dashboard labels the mode | banner, header tagline, real-data panels | `scripts\start_frontend.ps1 -RealData` | `scripts\ui_check.py --real` | 7/7 tabs PASS, 0 console errors, 0 failed requests; screenshots `data_real\reports\ui\` | VERIFIED |
| ML: no fabricated labels | ML audit in `docs/ml_model.md` | — | DQ check "ML training set" = not applicable in real mode | no real label exists; proxy objective proposed, not implemented | DONE (decision pending) |
| Hybrid mode: fill the missing sources with synthetic data (owner's request) | `producers/hybrid_generator.py` (simulator driven by real ERA5 rainfall), `data_hybrid/`, `api-hybrid` :8002, dashboard :5175 | `scripts\run_hybrid_data.ps1` | `tests/unit/test_hybrid.py` (5), `test_etl_in_hybrid_mode_scores_zones_and_keeps_provenance` | 282,408 records, 0 rejected, all applicable checks PASS; 0 zone-hours "Insufficient data", risk peak 73.5 on 2023-12-04; model trained (test set lacks Critical, documented); dashboard 7/7 | VERIFIED |
| Synthetic mode still works | — | `scripts\run_e2e.ps1`, `scripts\ui_check.py` | full suite | e2e 14/14 PASS (after the lake-reader fix, see "End-to-end"); backfill-only ML reproduces 0.677 exactly; synthetic dashboard 7/7 | VERIFIED |

## Known limitations (not fixed, documented)

Synthetic mode uses simulated data; real mode is historical batch only and
lacks real water level, traffic, reports and severity labels; single-node deployment; at-least-once delivery (no
end-to-end exactly-once); events older than the watermark do not update live
windows; streaming latency ~20 s per micro-batch on the Windows bind mount;
ML "High" class recall 0.28; ~8 GB Docker memory needed for the full stack.
