# AEGIS — feature traceability

Every feature → where it is implemented → what tests it → what was observed
when it ran. Evidence dates: 2026-10-21 (full verification) and 2026-10-22
(re-verification after the real-data and hybrid work). Course-requirement
evidence (Kafka, HBase, RDD, SQL, streaming, MLlib…) is also in
[traceability.md](traceability.md).

**Status classes**
* **IMPLEMENTED AND TESTED** — automated test(s) *and* an observed run.
* **IMPLEMENTED BUT NOT FULLY TESTED** — code exists and/or ran, but part of the behaviour has no automated test or no recorded run.
* **PARTIALLY IMPLEMENTED** — some of the described behaviour is missing.
* **CONFIGURED BUT NOT VERIFIED** — configuration exists, behaviour not observed.
* **DOCUMENTATION ONLY** — described (e.g. as a future option), no code.
* **NOT IMPLEMENTED** — not present.

Test kinds: **U** = unit (no services) · **S** = Spark (container) ·
**I** = integration (live Kafka/HBase/Spark) · **E2E** = `scripts/e2e_check.py`
step · **UI** = `scripts/ui_check.py` browser check.

## 1. Data generation and ingestion

| Feature | Source files and functions | Input | Processing | Output | Test evidence | Runtime verification | Status |
|---|---|---|---|---|---|---|---|
| Configuration + env overrides | `aegis/config.py: load_config, _apply_env_overrides, _validate` | `config/aegis.yaml`, `AEGIS__*` | YAML parse, typed overrides, validation | dict | U `test_config.py` (5) | every run | IMPLEMENTED AND TESTED |
| Synthetic world + backfill | `producers/world.py`, `data_generator.py: Simulator, generate_historical` | seed, days | storms → wetness → rivers/traffic/incidents | 4 JSONL files + manifest + registry | U `test_generator.py` (deterministic output, synthetic marking, label thresholds) | 296,817 lines (E2E 04) | IMPLEMENTED AND TESTED |
| Six raw formats | `producers/formats.py` | world state | encoders with different units/timestamps | JSON dicts | S `test_weather_formats_map_to_canonical_units_and_utc`, `test_traffic_local_time…` | E2E 05–08 | IMPLEMENTED AND TESTED |
| Defect injection + manifest | `producers/anomalies.py: AnomalyInjector`, `LiveGenerator._finalize` | events | 8 field defects, late/future times, counted | defective payloads, manifest counts | U `test_anomaly_injector_kinds`; S `test_etl_reconciliation_and_injected_defects` | injected = detected 8/8 kinds | IMPLEMENTED AND TESTED |
| Live generator (wall-clock stamps, 18× clock) | `data_generator.py: LiveGenerator` | config | heap-scheduled sensors, restamping | `Emitted` events | U `test_live_generator_stamps_wall_clock_time` | producers service ran > 30 min at 4 events/s | IMPLEMENTED AND TESTED |
| Real-data import (ERA5, IMD, NOAA, DFO, GDACS) | `aegis/realdata/*`, `ingest.py` | HTTP APIs/files | validate, normalise, provenance, SHA-256 cache | `data_real/raw/historical/*.jsonl` | U `test_realdata.py` (15 + parametrised) | 57,600 + 2,460 + 26 records; re-run byte-identical | IMPLEMENTED AND TESTED |
| Hybrid fill | `producers/hybrid_generator.py: ERA5RainEnvironment, generate_hybrid` | real ERA5 records | simulator driven by real rain | `data_hybrid/` | U `test_hybrid.py` (5); S `test_etl_in_hybrid_mode…` | 282,408 records, 0 rejected | IMPLEMENTED AND TESTED |

## 2. Kafka

| Feature | Source | Input | Processing | Output | Test evidence | Runtime | Status |
|---|---|---|---|---|---|---|---|
| Topic administration | `producers/kafka_admin.py: create_topics, describe_topics` | config | create 6 topics × 3 partitions, retention | topics | I `test_aegis_topics_exist` | E2E 02 | IMPLEMENTED AND TESTED |
| Producer (acks=all, idempotent, keyed) | `producers/kafka_client.py: AegisProducer`, `base_producer.run` | events | produce, callbacks, back-pressure | messages | I `test_produce_and_consume_round_trip` | E2E 05: 526 sent = 526 delivered | IMPLEMENTED AND TESTED |
| Dead-letter file + replay | `kafka_client._write_dead_letter`, `producers/replay_dead_letter.py` | failed deliveries | append JSONL; re-publish | `data/dead_letter/*.jsonl` | none | no failure occurred in recorded runs (failed = 0) | IMPLEMENTED BUT NOT FULLY TESTED |
| Consumer verification | `producers/kafka_consumer_check.py` | topics | consumer group read, JSON parse | counts | I round-trip test | E2E 06: 500 consumed | IMPLEMENTED AND TESTED |
| API live-feed consumer + alerts | `backend/app/services/live_feed.py` | `zone-risk-updates`, `data-quality-events` | bounded event log, High/Critical alerts | SSE events | none automated for SSE | UI: "live updates connected", alerts listed on Overview | IMPLEMENTED BUT NOT FULLY TESTED |
| Broker replication / multi-broker | `docker-compose.yml` (1 broker, RF 1) | — | — | — | — | — | NOT IMPLEMENTED (single broker by design) |

## 3. Spark processing

| Feature | Source | Input | Processing | Output | Test evidence | Runtime | Status |
|---|---|---|---|---|---|---|---|
| Canonical mapping + 12 rejection codes + flags | `spark_jobs/transforms.py: canonicalize` | raw records | parse, cast, map units/time/categories, zone, validate | canonical rows | S `test_transforms.py` (9 tests, 15 rejection cases) | ETL and streaming runs | IMPLEMENTED AND TESTED |
| Streaming `bronze_dq` | `streaming_pipeline.py: bronze_dq_batch` | Kafka | archive raw, quarantine, DQ metrics | bronze, quarantine, `live/dq_metrics`, DQ topic, HBase metrics | I `test_streaming_end_to_end` (bronze count, quarantine reasons) | E2E 07 | IMPLEMENTED AND TESTED |
| Streaming `silver` (watermark + dedup) | `start_query("silver")`, `silver_batch` | valid rows | `withWatermark`, `dropDuplicatesWithinWatermark` | `silver_stream/*`, HBase | I (dedup asserted, HBase row matches) | E2E 07 | IMPLEMENTED AND TESTED |
| Streaming `zone_risk` (windows + score) | `start_query("zone_risk")`, `zone_risk_batch` | valid rows | station join, 5-min windows (update), risk UDF | `live/zone_risk`, HBase, topic, snapshot | I (risk in range, topic populated) | E2E 07, 11 (live max 97.67) | IMPLEMENTED AND TESTED |
| Late data handling | watermark + `late_arrival` flag | late events | drop from state, keep in bronze | counts | S `test_late_arrival_flag` | `status.json` "dropped_by_watermark" > 0 | IMPLEMENTED AND TESTED |
| Checkpoint restart + back-off | `Pipeline.run`, `restart_delay_s` | failures | restart from checkpoint 10/20/40/60 s | — | S `test_restart_backoff_schedule` | 75 s HBase outage recovered (2026-10-21) | IMPLEMENTED AND TESTED |
| Idempotent Parquet appends on replay | `BatchMarkers` | batch id | skip already-committed appends | — | none | replay not observed directly | IMPLEMENTED BUT NOT FULLY TESTED |
| Graceful shutdown | `Pipeline.shutdown`, STOP file, SIGTERM | stop request | wait for in-flight batch | exit 0 | none | 37 s, exit 0 (2026-10-21) | IMPLEMENTED BUT NOT FULLY TESTED |
| Exact batch record counts | `kafka_records_in_batch` | progress JSON | sum offset ranges | status | S `test_kafka_records_from_offset_ranges` | Pipeline tab | IMPLEMENTED AND TESTED |
| Batch ETL bronze → silver → gold | `etl_pipeline.py: run` | JSONL + bronze | canonicalize, dedup, gold builders | lake tables, report | S `test_etl_reconciliation…`, `test_etl_outputs_readable` | 296,817 records, 9/9 checks | IMPLEMENTED AND TESTED |
| DQ reconciliation report | `etl_pipeline.dq_report` | canonical rows, manifest | count identities, injected vs detected | `dq_report_batch.json` | S (above) | all checks PASS | IMPLEMENTED AND TESTED |
| RDD transformations/actions | `spark_jobs/rdd_demo.py` | JSONL | 20 logged operations, lineage | `rdd_demo.json` | S `test_rdd_demo_runs_and_matches_dataframe` | E2E 10 | IMPLEMENTED AND TESTED |
| Spark SQL (10 queries) | `spark_jobs/sql_analytics.py` | lake views | joins, CTEs, windows, LAG, RANK | `reports/sql/*.json` | S `test_sql_queries_execute` | E2E 10, 37.8 s | IMPLEMENTED AND TESTED |
| Lake compaction | `spark_jobs/compact_lake.py` | streaming tables | rewrite closed days, count check | fewer files | none | no recorded run | IMPLEMENTED BUT NOT FULLY TESTED |
| Exactly-once end to end | — | — | — | — | — | — | NOT IMPLEMENTED (not claimed) |

## 4. Storage

| Feature | Source | Input | Processing | Output | Test evidence | Runtime | Status |
|---|---|---|---|---|---|---|---|
| HBase table design + row keys | `aegis/storage/hbase_repository.py: TABLES, *_row_key` | — | fixed-width/reverse timestamps, series | 5 tables | U `test_hbase_keys.py` (4) | `create_tables.py` | IMPLEMENTED AND TESTED |
| HBase put/get/scan/versions/delete | `HBaseRepository` | rows | Thrift calls with retries | cells | I `test_put_get_scan_versions_delete`, `test_unreachable_hbase_raises_clear_error` | `verify_hbase.py` all passed (E2E 03) | IMPLEMENTED AND TESTED |
| Spark → HBase sinks with count + read-back | `spark_jobs/hbase_sink.py: write, verify_sample` | DataFrames | per-partition batched puts | rows | I (streaming test reads row back) | ETL load: 7,104 incident / 22,556 sensor / 56,420 zone-risk rows (2026-10-21) | IMPLEMENTED AND TESTED |
| Parquet lake (bronze/silver/gold/live/quarantine) | ETL, streaming | — | partitioned writes | Parquet | S `test_etl_outputs_readable` | E2E 09: 7 tables read back with pyarrow | IMPLEMENTED AND TESTED |
| API lake reader (fast listing, cached counts) | `backend/app/services/parquet_store.py` | Parquet files | os.walk listing, pyarrow factory, parallel footers | DataFrames, counts | U `test_parquet_store.py` (2) | overview 75 s → 19 s cold | IMPLEMENTED AND TESTED |

## 5. Analytics, risk, spatial, ML

| Feature | Source | Input | Processing | Output | Test evidence | Runtime | Status |
|---|---|---|---|---|---|---|---|
| Rule-based risk formula | `aegis/risk.py: assess` | indicator values | ramps, weights, coverage, categories, explanation | assessment | U `test_risk.py` (8); S `test_risk_udf_matches_python_reference` | batch 0–96.99, live 0–97.67 (E2E 11) | IMPLEMENTED AND TESTED |
| Missing ≠ safe (coverage, Insufficient data) | `assess`, `reports_observed`, `hazard_index` | missing indicators | exclude from weights; NULL hazard | "Insufficient data" | U `test_risk.py`; S `test_missing_report_source_is_not_observed`, `test_hazard_index_is_null…` | real mode 57,600/57,600 Insufficient | IMPLEMENTED AND TESTED |
| Zone signal integration | `risk_analytics.zone_signals`, `aggregate_signals` | canonical rows | station join, windows | zone windows | S `test_zone_signals_with_missing_domains` | gold/live tables | IMPLEMENTED AND TESTED |
| Zone grid + Haversine (Python/numpy/Spark parity) | `aegis/geo.py`, `transforms.zone_id_col`, `haversine_km_col` | coordinates | floor grid, great-circle | zone id, km | U `test_geo.py`; S `test_zone_id_parity_with_python` (409 points), `test_haversine_column_matches_python` | — | IMPLEMENTED AND TESTED |
| Nearest-station assignment | `spatial_analytics.nearest_station_map` | zones × stations | cross join, rank, radius | zone → station | S (ETL tests) | 39/40 zones weather, 19/40 gauge | IMPLEMENTED AND TESTED |
| Nearby hazard search | `incident_service.nearby`, `spatial_analytics.nearby` | point, radius | Haversine, exclude rejected | sorted list | U `test_nearby_uses_haversine_and_excludes_rejected` | 236 hazards within 3 km | IMPLEMENTED AND TESTED |
| GeoJSON output | — | — | — | — | — | — | NOT IMPLEMENTED (zones served as `bounds`) |
| Point-in-time ML features | `spark_jobs/features.py` | buckets, reports | look-back joins | 9 features | S `test_ml_features_use_only_past_data`; U `test_ml_contract.py` | 1,822 training rows | IMPLEMENTED AND TESTED |
| Model training + evaluation + persistence | `model_training.py: train, evaluate, time_split` | gold training set | validate, split, fit, evaluate, save, reload | model + metadata | S `test_model.py` (5) | accuracy 0.677, reload passed (E2E 12) | IMPLEMENTED AND TESTED |
| Evaluation with a class missing from the test set | `model_training.evaluate` | predictions | per-class from confusion matrix, null for absent class | metrics | (hybrid run) | hybrid model trained; synthetic values identical (12/12) | IMPLEMENTED BUT NOT FULLY TESTED (no dedicated unit test) |
| Zone inference + API prediction | `model_inference.py: SeverityModel, score_zones`; `model_service.py` | features | predict, insufficient-data rule | predictions, HBase rows | S `test_saved_model_reloads_and_predicts`, `test_missing_model_reports_unavailable`; U `test_model_unavailable_returns_503` | 39/40 zones predicted; POST predict 200 (E2E 13–14) | IMPLEMENTED AND TESTED |
| Real-data ML model | — | — | — | — | — | — | NOT IMPLEMENTED (no real labels; proxy objective documented only) |
| Real-data layers (IMD vs ERA5, gauge, events) | `spark_jobs/real_data_layers.py` | real files | daily joins, both day alignments | gold + validation report | S `test_etl_and_real_layers_in_real_mode` | r = 0.784 ERA5 vs IMD | IMPLEMENTED AND TESTED |

## 6. Serving and presentation

| Feature | Source | Input | Processing | Output | Test evidence | Runtime | Status |
|---|---|---|---|---|---|---|---|
| 16 REST/SSE endpoints | `backend/app/api/routes.py` | HTTP | services | JSON / SSE | U `test_api.py` (7), `test_api_realmode.py` (3) | E2E 14: every endpoint 200 | IMPLEMENTED AND TESTED |
| Fallback HBase → snapshot → gold, `stale`/`data_origin` | `zone_service.current` | sources | newest window wins | zones | U `test_zones_fall_back_to_batch_and_flag_origin` | HBase outage test | IMPLEMENTED AND TESTED |
| Data-mode labelling (synthetic/real/hybrid) | `dataset_service.py`, frontend `dataMode` | manifest | — | banners, taglines | U `test_dataset_endpoint_reports_real_mode`, `test_dataset_service_labels_hybrid_mode` | UI 7/7 in all three modes | IMPLEMENTED AND TESTED |
| Pipeline monitoring | `pipeline_service.py`, `StatusBoard`, `ProgressListener` | Kafka, HBase, status files | issues list | status JSON, Pipeline tab | U `test_pipeline_status_reports_degraded` | `overall: ok` | IMPLEMENTED AND TESTED |
| Dashboard (7 tabs, map, charts) | `frontend/src/**` | API | React rendering | UI | UI check (no component unit tests) | 7/7 tabs, 0 console errors (synthetic, real, hybrid) | IMPLEMENTED BUT NOT FULLY TESTED (browser check only) |
| Authentication / TLS | — | — | — | — | — | — | NOT IMPLEMENTED (local demo) |

## 7. Operations

| Feature | Source | Test evidence | Runtime | Status |
|---|---|---|---|---|
| Docker images + Compose services | `docker/`, `docker-compose.yml` | all container tests run in `aegis-spark` | every service ran; health checks pass | IMPLEMENTED AND TESTED |
| PowerShell entry points | `scripts/*.ps1` | — | all 12 synthetic-mode scripts exit 0 (2026-10-21); real/hybrid scripts exit 0 (2026-10-22) | IMPLEMENTED AND TESTED |
| End-to-end check | `scripts/e2e_check.py`, `run_e2e.ps1` | — | 14/14 PASS (2026-10-22 13:28 UTC) | IMPLEMENTED AND TESTED |
| Diagram rendering | `scripts/render_diagrams.py` | — | rendered all 3 diagrams (2026-10-22) | IMPLEMENTED BUT NOT FULLY TESTED (needs internet for the Mermaid CDN) |
| GloFAS proxy water indicator, NDMA SACHET live warnings, Kafka replay of real history | `docs/dataset_migration_plan.md` § 6 | — | sources verified reachable; no code | DOCUMENTATION ONLY |
