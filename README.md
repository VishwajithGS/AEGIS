# AEGIS — Real-Time Disaster Intelligence and Risk Analytics

*A real-time disaster intelligence and risk analytics platform using Apache
Kafka, Apache Spark, HBase and Spark MLlib.* Big Data and Modern Databases —
final package (V Semester M.Sc).

AEGIS ingests heterogeneous disaster-related event streams (weather, river
levels, traffic, citizen incident reports) through **Kafka**, validates,
standardises and integrates them with **Spark** (RDDs, Spark SQL, Structured
Streaming), computes explainable **rule-based zone risk scores** and spatial
analytics, stores operational data in **HBase** (wide-column) and history in
partitioned **Parquet**, trains a supporting **Spark MLlib** severity
classifier, and serves everything through a **FastAPI** backend and a
**React + Leaflet** dashboard.

> **Three data modes, each in its own data root and always labelled.**
> * **Synthetic (default, `data/`)** — a deterministic simulator over a demo
>   bounding box around Chennai drives the full live stack. No synthetic
>   record is a real sensor reading, official report or verified incident.
> * **Real historical (`data_real/`)** — verified public datasets for
>   2023-11-01 – 2023-12-30 (Cyclone Michaung): ERA5 reanalysis hourly
>   rainfall, IMD gridded and NOAA gauge daily rainfall, DFO/GDACS flood
>   events. Batch only, no live feed. Water level, traffic and citizen reports
>   have **no verified open real source** and are shown as not available.
>   See [Real historical data mode](#real-historical-data-mode).
> * **Hybrid (`data_hybrid/`)** — the real rainfall and events above, plus
>   **synthetic** water level, traffic, citizen reports and incidents that the
>   simulator generates *from the real ERA5 rainfall*, so every zone gets a
>   risk score and the ML model can be trained. Every synthetic record is
>   tagged `synthetic: true`, `data_source = aegis-simulator-era5-driven`.
>   See [Hybrid mode](#hybrid-mode-real-rainfall--synthetic-fill).
>
> Risk scores are a transparent demonstration index, not an official forecast
> or warning. ML labels are synthetic in both modes; no real label exists
> (see [docs/ml_model.md](docs/ml_model.md)).

## Why it matters

Urban flooding produces signals in many systems at once: rain gauges, river
sensors, traffic feeds and citizen reports. They differ in format, units,
timing and reliability, and they arrive continuously. AEGIS shows how to turn
these high-volume, multi-source streams into one consistent, explainable,
time-aware picture of risk per area, while remaining honest about missing
or late data.

## Objectives

1. Ingest multiple semi-structured, time-series sources through Kafka.
2. ETL with validation, mapping, standardisation, de-duplication and quarantine.
3. Demonstrate Spark RDD transformations/actions, Spark SQL and Structured Streaming.
4. Compute explainable zone risk and spatial analytics; store in HBase and Parquet.
5. Train, evaluate and serve a small Spark MLlib classifier as a supporting component.
6. Present live and historical results on a dashboard; verify everything with tests.

## Architecture (summary)

```
synthetic producers ─▶ Kafka (4 source topics) ─▶ Spark Structured Streaming ─┬─▶ bronze / quarantine / DQ metrics
historical JSONL (60 days) ─▶ Spark batch ETL ─▶ silver ─▶ gold                ├─▶ silver_stream + HBase sensor/incident rows
                                   │                                           └─▶ zone risk windows ─▶ HBase + Parquet + Kafka
                     RDD demo · Spark SQL · spatial · risk · MLlib            zone-risk-updates ─▶ FastAPI (SSE) ─▶ React/Leaflet
```

Full description (topics, ETL layers, streaming semantics, watermarks,
guarantees, spatial model, risk formula, HBase schema, Parquet layout):
**[docs/architecture.md](docs/architecture.md)**.

**Review pack** (code-verified, for the presentation and viva):
[project_codebase_guide.md](docs/project_codebase_guide.md) (A–Z explanation) ·
[technology_stack_guide.md](docs/technology_stack_guide.md) (why each tool) ·
[end_to_end_workflow.md](docs/end_to_end_workflow.md) (start-up + one event traced) ·
[project_runbook.md](docs/project_runbook.md) (commands) ·
[presentation_and_viva_guide.md](docs/presentation_and_viva_guide.md) (talk, demo, Q&A) ·
[feature_traceability.md](docs/feature_traceability.md) (feature → code → test → evidence) ·
diagrams [architecture.png](docs/architecture.png), [runtime_sequence.png](docs/runtime_sequence.png),
[batch_ml_workflow.png](docs/batch_ml_workflow.png) (Mermaid sources `docs/*.mmd`) ·
slides [AEGIS_architecture.pptx](docs/AEGIS_architecture.pptx).

## Technology stack (versions in use)

| Component | Version | Notes |
|---|---|---|
| Python | 3.11 (container `python:3.11-slim-bookworm`; host 3.11.9) | |
| Java | OpenJDK 17 (container) | Spark 3.5 does not support Java 24 (host default) |
| Apache Spark / PySpark | 3.5.9 | RDD, SQL, Structured Streaming, MLlib |
| Spark–Kafka connector | spark-sql-kafka-0-10_2.12 3.5.9, kafka-clients 3.4.1 | pre-installed in the image |
| Apache Kafka | 4.3.1 (official `apache/kafka`, KRaft, single broker) | |
| Apache HBase | 2.5.16 standalone + Thrift (built from the official binary, SHA-512 checked) | `docker/hbase` |
| Python clients | confluent-kafka 2.16.0, happybase 1.3.0 / thriftpy2 0.7.1, pyarrow 18.1, pandas 2.2.3 | |
| API | FastAPI 0.151, Uvicorn 0.54, pydantic 2.14 | |
| Dashboard | React 18.3.1, Vite 5.4.11, Leaflet 1.9.4 / react-leaflet 4.2.1, Recharts 2.13.3 | |
| Orchestration | Docker Compose v5, PowerShell scripts | |

## Repository structure

```
aegis/            shared library: config, logging, geo, risk formula, canonical vocabularies,
                  ML feature contract, storage/hbase_repository.py,
                  realdata/ (real-data adapters: ERA5, IMD NetCDF, GHCN-D, DFO, GDACS; ingest CLI)
producers/        world simulator, source formats, anomaly injection, generator CLI, hybrid_generator
                  (synthetic fill driven by real ERA5 rainfall),
                  Kafka producer/admin/consumer-check, per-source producers, run_all, dead-letter replay
spark_jobs/       transforms (mapping+validation), etl_pipeline, rdd_demo, sql_analytics,
                  streaming_pipeline, risk_analytics, spatial_analytics, features,
                  model_training, model_inference, hbase_sink, compact_lake, common,
                  real_data_layers (real-data gold tables + validation report)
hbase/            create_tables.py, verify_hbase.py
backend/app/      FastAPI: main, api/routes, services (zones, incidents, analytics, pipeline, model, live feed)
frontend/         React + Vite dashboard (pages, components, hooks, services/api.js)
config/aegis.yaml central configuration (all weights, thresholds, rates, paths)
docker/           spark/Dockerfile, hbase/{Dockerfile, hbase-site.xml, entrypoint.sh}
scripts/          PowerShell entry points + e2e_check.py, ui_check.py, render_diagrams.py
tests/            unit/, spark/, integration/
docs/             architecture, data dictionary, API, ML model, testing, demo guide, traceability,
                  dataset audit, dataset migration plan, review pack (codebase guide, technology guide,
                  workflow, runbook, viva guide, feature traceability), diagrams (.mmd/.svg/.png), slides (.pptx)
data/             synthetic mode: generated data, lake, checkpoints, state, reports, models (git-ignored)
data_real/        real-data mode: downloaded raw responses, normalised records, lake, reports (git-ignored)
data_hybrid/      hybrid mode: real files copied from data_real + synthetic fill, lake, model, reports (git-ignored)
```

## Requirements

* Windows 10/11 with **Docker Desktop** (WSL 2 backend). Give Docker **≥ 8 GB
  RAM**: the full live stack plus a batch job peaked at ~6.3 GB of container
  memory on the verification machine. Docker's disk image needs ~10 GB free
  (images ≈ 2.8 GB plus data); keep it on a drive with space.
* **Node.js 18+** for the dashboard (tested with Node 24).
* Optional: Python 3.11 on the host for the `.venv` tools/tests.
* Internet access on first build (images, Maven jars, npm packages) and for map tiles.
  On a slow link, pre-download the 318 MB PySpark archive with a resumable tool
  (see [docker/spark/wheels/README.md](docker/spark/wheels/README.md)).
* Optional for the browser UI check: Playwright in the host venv
  (`.\.venv\Scripts\python.exe -m pip install playwright==1.55.0`); it uses the installed Edge or Chrome.

## Setup and execution (PowerShell, project root)

Run each command from `D:\SEM 5\AEGIS` (adjust to your path). If script
execution is restricted, prefix with `powershell -ExecutionPolicy Bypass -File`.

| Step | Terminal | Command | What it does |
|---|---|---|---|
| 1 | 1 | `scripts\setup.ps1` (add `-HostVenv` for a host venv) | checks Docker/Node, builds `aegis-spark` and `aegis-hbase`, installs the dashboard |
| 2 | 1 | `scripts\start_infrastructure.ps1` | starts Kafka + HBase, waits for health, creates topics and tables |
| 3 | 1 | `scripts\generate_data.ps1` | 60-day synthetic backfill + `data\sample` |
| 4 | 1 | `scripts\run_batch_analytics.ps1` | batch ETL (+ HBase load), RDD demo, Spark SQL, spatial, risk ranking |
| 5 | 1 | `scripts\train_model.ps1` | MLlib training/evaluation/save/reload + sample prediction |
| 6 | 1 | `scripts\start_streaming.ps1` | Structured Streaming in the background (Spark UI http://localhost:4040) |
| 7 | 1 | `scripts\start_producers.ps1` | live synthetic events (4/s) to Kafka |
| 8 | 1 | `scripts\start_api.ps1` | FastAPI on http://localhost:8000 (Swagger: /docs) |
| 9 | 2 | `scripts\start_frontend.ps1` | dashboard on http://127.0.0.1:5173 (keep running) |
| 10 | 1 | `scripts\train_model.ps1 -Infer` (after a few minutes of streaming) | ML predictions for every zone from live data |
| 11 | 1 | `scripts\run_tests.ps1` | unit, Spark and integration tests |
| 12 | 1 | `scripts\run_e2e.ps1` | full end-to-end check → `data\reports\e2e_report.md` (pauses and restores `streaming`/`producers` if they are running) |
| 13 | 1 | `.\.venv\Scripts\python.exe scripts\ui_check.py` (optional) | opens every dashboard tab in a real browser, screenshots → `data\reports\ui\` |
| — | 1 | `scripts\stop_all.ps1` (`-Down` to remove containers, `-Down -DeleteVolumes` also wipes Kafka/HBase data) | graceful stop |

Individual demos: `scripts\run_batch_analytics.ps1 -Only rdd|sql|spatial|risk|etl`;
one producer: `scripts\start_producers.ps1 -Source weather -Rate 1 -Duration 60`;
drain Kafka once: `scripts\start_streaming.ps1 -Once`; dashboard tabs can be
opened directly (`http://127.0.0.1:5173/#map`, `#zones`, `#model`, `#pipeline`, …); faculty walkthrough:
**[docs/demo_guide.md](docs/demo_guide.md)**.

Equivalent raw commands (any shell): `docker compose up -d kafka hbase`,
`docker compose run --rm tools python -m spark_jobs.etl_pipeline --write-hbase`,
`docker compose run --rm --no-deps tools python -m spark_jobs.rdd_demo`, etc.

## Real historical data mode

Replaces the synthetic inputs with verified public datasets wherever one
exists, in a separate data root (`data_real/`), with provenance on every
record. Audit of every synthetic dataset: [docs/dataset_audit.md](docs/dataset_audit.md);
sources, evidence, mapping, ML audit and results:
[docs/dataset_migration_plan.md](docs/dataset_migration_plan.md).

| Step | Terminal | Command | What it does |
|---|---|---|---|
| R1 | 1 | `scripts\ingest_real_data.ps1` (options `-Start 2023-11-01 -End 2023-12-30 -Sources era5,imd,ghcnd,dfo,gdacs -Refresh`) | downloads ERA5 (Open-Meteo), IMD gridded NetCDF, NOAA GHCN-D, DFO, GDACS; validates and normalises them into `data_real\raw\historical\`. No account or API key needed. Responses are cached, so a re-run is offline and byte-identical |
| R2 | 1 | `scripts\run_real_data.ps1` (`-NoApi` to skip the API) | batch ETL in real mode → `data_real\lake`, real-data gold tables and `data_real\reports\real_data_validation.json`, then the real-mode API container `api-real` on http://localhost:8001 |
| R3 | 2 | `scripts\start_frontend.ps1 -RealData` | dashboard on http://127.0.0.1:5174 against the real-mode API (keep running) |
| R4 | 1 | `.\.venv\Scripts\python.exe scripts\ui_check.py --real --url http://127.0.0.1:5174 --out data_real/reports/ui` (optional) | browser check of all tabs in real mode |

Requires Kafka and HBase only for the synthetic stack; the real mode needs
Docker (Spark image) and internet access for step R1. Both modes can run at
the same time (API 8000/dashboard 5173 = synthetic, 8001/5174 = real).

What the real mode shows: ERA5 hourly rainfall per zone through the same
ETL, risk and analytics code; IMD/NOAA observed daily rainfall compared with
ERA5; officially reported flood events. What it does not: live streaming,
water level, traffic, citizen reports, incident-level severity, ML
predictions. With rainfall as the only observed indicator, every zone-hour
is **Insufficient data** — the risk index does not pretend the other
signals were zero.

## Hybrid mode (real rainfall + synthetic fill)

Fills the gaps of the real mode with synthetic data so that the whole
pipeline (risk with all indicators, incidents, MLlib) runs on the real
cyclone period. The simulator's own random storms are replaced by the **real
ERA5 hourly rainfall** of each zone, so simulated rivers rise, roads slow down
and incidents happen when the real rain fell. Nothing synthetic is presented
as real: the dashboard shows an amber "Hybrid data" banner listing the real
and the synthetic parts, and every synthetic record carries its own tag.

| Step | Terminal | Command | What it does |
|---|---|---|---|
| H1 | 1 | `scripts\ingest_real_data.ps1` (once) | real data → `data_real\` (see above) |
| H2 | 1 | `scripts\run_hybrid_data.ps1` (`-NoApi`, `-Seed 42`, `-WetnessTauH 24`) | synthetic fill → `data_hybrid\`, batch ETL, real-data layers, MLlib training + zone predictions, API `api-hybrid` on http://localhost:8002 (~6 min; pauses and restores `streaming`/`producers` for memory) |
| H3 | 2 | `scripts\start_frontend.ps1 -Hybrid` | dashboard on http://127.0.0.1:5175 (keep running) |
| H4 | 1 | `.\.venv\Scripts\python.exe scripts\ui_check.py --hybrid --url http://127.0.0.1:5175 --out data_hybrid/reports/ui` (optional) | browser check of all tabs |

What to expect (2023-11-01 – 2023-12-30): flood incidents peak on 4–5
December, simulated rivers reach ~1.4 × danger level on 4 December, and the
zone risk peaks at ~73 (High) while the rest of the period is mostly Low.
The Overview tab shows the *latest* window (31 December, after the cyclone);
use Historical analytics for the cyclone. Risk scores mix real and synthetic
indicators: they demonstrate the pipeline, they do not assess what happened.

## Configuration

Everything tunable lives in [config/aegis.yaml](config/aegis.yaml): Kafka
servers/topics/producer settings, Spark settings, streaming trigger, window
and **watermark**, storage paths, HBase endpoint and failure policy,
**risk weights, normalisation ramps, category and confidence thresholds**,
alert categories, generator rates/seed/anomaly rate, and model parameters.
Override any value with `AEGIS__SECTION__KEY` (see [.env.example](.env.example));
the compose file sets the container endpoints (`kafka:29092`, `hbase:9090`).
Dashboard: `frontend/.env.local` → `VITE_API_BASE_URL`.

## What each requirement looks like in AEGIS

* **Kafka:** keyed JSON events, 3 partitions/topic, `acks=all` + idempotence,
  delivery callbacks, dead-letter file + replay; derived topics for zone risk and DQ.
* **ETL:** bronze (raw, nothing dropped) → silver (canonical, UTC, metric units,
  normalised categories, zone, flags, unique ids) → gold (aggregates, risk,
  trends, correlations, ML set). Rejected records go to quarantine with reasons;
  the DQ report reconciles counts and compares detected defects with the
  generator's injected-defect manifest.
* **RDD:** `textFile, map, filter, flatMap, reduceByKey, aggregateByKey,
  mapValues, distinct, sortBy` and `count, first, take, collect, reduce`, with
  lineage and an RDD-vs-DataFrame cross-check.
* **Spark SQL:** 10 queries over temp views (joins, CTEs, `window()`, `LAG`,
  `RANK`, moving averages, `LATERAL VIEW explode`).
* **Structured Streaming:** 3 checkpointed queries; watermark 10 min,
  5-minute event-time windows in update mode; dedup with
  `dropDuplicatesWithinWatermark`; foreachBatch sinks to Parquet, HBase and Kafka;
  listener-based monitoring; restart from checkpoints; graceful stop.
* **Risk:** `risk = 100·Σ wᵢfᵢ / Σ wᵢ (available)`; missing indicators reduce
  coverage/confidence and never imply safety; per-indicator contributions
  and plain-language explanations.
* **Spatial:** 40-cell analysis grid, Haversine distances, nearest-station
  assignment via a Spark spatial join, nearby-hazard search, incident density,
  time comparisons.
* **HBase:** 5 tables with documented row keys (fixed-width timestamps,
  reverse timestamps), multi-version risk cells, TTLs, sparse rows, Thrift client,
  read-back verification.
* **MLlib:** Decision Tree pipeline, time-based split, held-out metrics,
  confusion matrix, persistence + reload check, API predictions persisted to HBase.

## Testing

See [docs/testing.md](docs/testing.md). `scripts\run_tests.ps1` runs host unit
tests, then unit + Spark tests in the container, then integration tests
against the live services (skips are reported with their reason).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `docker` commands hang; Docker Desktop logs show `I/O error`, `dockerd` SIGBUS | the drive holding Docker's disk image is full → free space or move the disk image (Settings → Resources → Advanced) |
| `Java gateway process exited` / `UnsupportedOperationException: getSubject` on the host | host Java is 24; Spark 3.5 needs Java 17 → use the container (or set `JAVA_HOME` to a JDK 17) |
| Spark on Windows: `HADOOP_HOME and hadoop.home.dir are unset` when writing | writing files from native Windows Spark needs Hadoop `winutils`; run jobs in the container |
| Producers: `cannot reach Kafka at localhost:9092` | `scripts\start_infrastructure.ps1`; check `docker compose ps` |
| Producers: `topics missing on broker` | `docker compose run --rm --no-deps tools python -m producers.kafka_admin create` |
| `HBase Thrift server not reachable` | HBase takes ~1 min to start; `docker compose logs hbase`; UI http://localhost:16010 |
| Streaming status says `hbase_sink: disabled: unreachable at startup` | HBase was down when streaming started → start HBase, then `docker compose restart streaming` |
| Dashboard: `Cannot reach the AEGIS API` | start the API (`scripts\start_api.ps1`); check `VITE_API_BASE_URL`; CORS origins in `AEGIS_API_CORS_ORIGINS` |
| `/api/model/predict` returns 503 | train first (`scripts\train_model.ps1`); host-run API cannot host the model (no Java 17) |
| Live zones all "STALE" | streaming or producers stopped; check Dashboard → Pipeline |
| Port already in use (9092, 9090, 16010, 8000, 4040, 5173) | stop the other program or change the published port in `docker-compose.yml` |
| Many small Parquet files after a long run; dashboard Overview slow | each micro-batch adds files, and the API reads them all. Stop streaming and run `docker compose run --rm --no-deps tools python -m spark_jobs.compact_lake` (compacts closed days of the streaming tables; bronze and `live/dq_metrics` are not compacted) |
| Real-mode dashboard shows the synthetic banner | it is talking to the synthetic API: start it with `scripts\start_frontend.ps1 -RealData` (port 5174 → API 8001) and check `docker compose --profile real ps api-real` |
| `scripts\ingest_real_data.ps1` exits with `ok: false` | a source failed (network, HTTP error, unexpected format); nothing is substituted. Re-run later; cached sources are not downloaded again. `-Sources era5,imd` limits the run to some sources |
| Image build fails with `ReadTimeoutError ... files.pythonhosted.org` | slow link to PyPI: pre-download the PySpark archive as described in `docker/spark/wheels/README.md`, then `docker compose build tools` |
| HBase container exits; its log shows `We slept ...ms instead of ...` and `Session expired` / `ABORTING master` | the host paused the JVM (memory pressure). Give Docker more memory or stop `streaming`/`producers` while running batch jobs; HBase restarts automatically (`restart: unless-stopped`) |
| ETL exits with code 1 and `hbase_load_failed=True` | HBase was unreachable during the load; check `docker compose ps hbase`, then re-run `scripts\run_batch_analytics.ps1 -Only etl` (writes are idempotent) |
| Streaming status `state: crashed` | the driver died (e.g. out of memory); Docker restarts it and it resumes from the checkpoints; see `docker compose logs streaming` |

## Known limitations

* Synthetic mode: all data is simulated; live data is time-compressed (a 6-hour storm in ~20 minutes).
* Real mode: historical batch only (2023-11-01 – 2023-12-30). Hourly rainfall is
  ERA5 reanalysis, not gauge observations; all 40 zones fall in only 2 ERA5
  cells and 3 IMD cells, so neighbouring zones share values. ERA5 totals are
  22 % below IMD for the period. Whether IMD's date D means the 24 h starting
  or ending at 08:30 IST is not confirmed; both alignments are reported. No
  real water level, traffic, reports or severity labels, so the risk index is
  "Insufficient data" throughout and no ML model is served.
* Hybrid mode: water level, traffic, reports, incidents and severity labels are
  synthetic. The simulator's wetness time constant is set to 24 h for ERA5's
  smooth rainfall (with the default 6 h the simulated rivers never reach danger
  level during Cyclone Michaung) — a synthetic-model setting, not a measured
  catchment property. With one cyclone in the period, the ML time split puts
  almost all severe incidents in training; the test set has 1 High and no
  Critical incident, so the hybrid model's accuracy (0.84) mostly reflects
  easy low-severity cases.
* Single-node deployment: one Kafka broker (replication 1), standalone HBase on
  the local filesystem, Spark in local mode (`local[8]` batch, `local[4]` streaming).
  No distributed scalability was measured.
* Streaming delivery is at-least-once; HBase writes are idempotent and Parquet
  appends are batch-guarded, but exactly-once end to end is not claimed.
* Events older than the 10-minute watermark do not update live windows (they
  are kept in bronze and the batch layer).
* Risk weights/thresholds are demonstration values, not calibrated hazard science.
* ML labels come from a known synthetic rule; metrics are not real-world accuracy
  (accuracy 0.677 vs 0.323 majority baseline; the "High" class is weak, recall 0.28).
* Streaming micro-batches take ~12–22 s on the Windows bind mount, so live
  results trail the events by ~20 s (the job keeps up at 4 events/s).
* No authentication/TLS (local demo only). Map tiles need internet access.
* Zones are a regular grid, not administrative boundaries; no road network is modelled.

## Verification status

Everything in this repository was run on the project machine on 2026-10-21
(Docker Desktop 29.8.0, 16 cores, 7.6 GB Docker VM):

| Check | Result |
|---|---|
| Tests (`scripts\run_tests.ps1`) | 55 host unit tests; 97 container tests (55 unit, 37 Spark, 5 integration against live Kafka/HBase/Spark) — all passed, none skipped |
| End-to-end (`scripts\run_e2e.ps1`) | 14/14 steps PASS (Kafka → Structured Streaming → batch ETL → Parquet/HBase → RDD/SQL → risk → MLlib → API) |
| Scripts | all 12 PowerShell entry points run as documented, exit 0; cold start after `stop_all.ps1` reaches pipeline status `ok` |
| Batch ETL | 296,817 backfill records; all 9 reconciliation checks pass; every injected defect detected (8/8 kinds) |
| ML (synthetic labels) | accuracy 0.677, weighted F1 0.664, majority baseline 0.323; save/reload verified |
| Live system | 4 events/s for >30 min; graceful stop 37 s; recovery after an HBase outage |
| Dashboard (`scripts\ui_check.py`) | 7/7 tabs render project data, 0 console errors |

Real historical data mode, run on 2026-10-22:

| Check | Result |
|---|---|
| Import (`scripts\ingest_real_data.ps1`) | 57,600 ERA5 hourly records (40 zones × 1,440 h, 0 missing), 2,400 IMD + 60 NOAA daily records (25 gauge days missing, kept missing), 26 official events; re-run byte-identical |
| Real pipeline (`scripts\run_real_data.ps1`) | exit 0; 57,600 records valid, 0 rejected; reconciliation checks pass (injected-defect and ML checks not applicable); 57,600/57,600 zone-hours "Insufficient data" |
| Source agreement (not model skill) | ERA5 vs IMD daily region mean r = 0.784, MAE 10.8 mm; NOAA gauge vs IMD cell r = 0.99 |
| Tests | 90 host unit tests; 138 container tests (90 unit incl. 25 real-data, 5 hybrid, 3 real-mode API, 2 lake-reader; 43 Spark incl. 6 real-data/hybrid; 5 integration) — all passed, none skipped |
| Hybrid mode (`scripts\run_hybrid_data.ps1`) | exit 0; 282,408 records, 0 rejected, all applicable checks pass; 0 of 57,600 zone-hours "Insufficient data" (risk peak 73.5 on 2023-12-04); model trained (accuracy 0.842 on a test set without Critical cases, see Known limitations); dashboard 7/7 tabs PASS |
| Synthetic mode after the migration | `scripts\run_e2e.ps1` 14/14 PASS; backfill-only ML reproduces accuracy 0.677 / weighted F1 0.664 exactly; synthetic dashboard 7/7 tabs PASS |
| Dashboard (`scripts\ui_check.py --real`) | 7/7 tabs PASS, 0 console errors, 0 failed requests |

Per-requirement evidence: [docs/traceability.md](docs/traceability.md) ·
ML details: [docs/ml_model.md](docs/ml_model.md) ·
handoff notes: [docs/handoff.md](docs/handoff.md).
