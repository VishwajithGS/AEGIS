# AEGIS — handoff notes (2026-10-21)

For a teammate or reviewer running AEGIS on another machine. Everything listed
as passed below was executed on the project machine (Windows 11, Docker
Desktop 29.8.0, 16 cores, 7.6 GB Docker VM). Nothing is estimated.

## 1. What AEGIS is

A real-time disaster intelligence and risk analytics platform (Big Data and
Modern Databases final package): synthetic weather, river-level, traffic and
citizen-report streams → **Kafka** → **Spark** (RDD, Spark SQL, Structured
Streaming, MLlib) → **HBase** + partitioned **Parquet** → **FastAPI** →
**React/Leaflet** dashboard. All data is synthetic.

* Overview, stack and versions: [README.md](../README.md)
* Architecture (topics, ETL layers, streaming semantics, risk formula, HBase schema, resources): [architecture.md](architecture.md)
* Data dictionary: [data_dictionary.md](data_dictionary.md) · API: [api.md](api.md) · ML: [ml_model.md](ml_model.md)
* Tests: [testing.md](testing.md) · Faculty demo script: [demo_guide.md](demo_guide.md) · Requirement evidence: [traceability.md](traceability.md)

## 2. Setup on a new machine (Windows, PowerShell)

Prerequisites: Docker Desktop (WSL 2) running with **≥ 8 GB memory** and its
disk image on a drive with **≥ 10 GB free**; Node.js 18+; Git; Python 3.11
(optional, for host tools).

```powershell
git clone https://github.com/VishwajithGS/AEGIS.git
cd AEGIS
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1 -HostVenv   # builds aegis-spark + aegis-hbase, installs dashboard, creates .venv
```

First build downloads ~2 GB (base images, PySpark, HBase, Maven jars, npm). On a
slow link see [docker/spark/wheels/README.md](../docker/spark/wheels/README.md).

Not in the repository (recreated by setup or the generator): `.venv/`,
`frontend/node_modules/`, all of `data/` except `data/sample/`.

## 3. Run order

| # | Command | Purpose |
|---|---|---|
| 1 | `scripts\setup.ps1 -HostVenv` | images, dashboard deps, host venv |
| 2 | `scripts\start_infrastructure.ps1` | Kafka + HBase, topics, tables |
| 3 | `scripts\generate_data.ps1` | 60-day synthetic backfill (seed 42) + sample |
| 4 | `scripts\run_batch_analytics.ps1` | batch ETL (+ HBase load), RDD, Spark SQL, spatial, risk |
| 5 | `scripts\train_model.ps1` | MLlib training, evaluation, save/reload |
| 6 | `scripts\start_streaming.ps1` | Structured Streaming (Spark UI :4040) |
| 7 | `scripts\start_producers.ps1` | live synthetic events (4/s) |
| 8 | `scripts\start_api.ps1` | FastAPI :8000 (/docs) |
| 9 | `scripts\start_frontend.ps1` (2nd terminal) | dashboard :5173 |
| 10 | `scripts\train_model.ps1 -Infer` | per-zone ML predictions from live data |
| 11 | `scripts\run_tests.ps1` | unit + Spark + integration tests |
| 12 | `scripts\run_e2e.ps1` | end-to-end check → `data\reports\e2e_report.md` |
| 13 | `.\.venv\Scripts\python.exe scripts\ui_check.py` (optional, needs `pip install playwright==1.55.0`) | dashboard check in a real browser |
| — | `scripts\stop_all.ps1` | graceful stop |

Prefix each with `powershell -ExecutionPolicy Bypass -File` if script execution is restricted.

## 4. Results on the project machine (2026-10-21)

| Check | Result |
|---|---|
| Host unit tests | 55 passed |
| Container tests (`pytest tests`) | 97 passed: 55 unit, 37 Spark, 5 integration against live Kafka/HBase/Spark; 0 skipped |
| `scripts\run_e2e.ps1` | 14/14 PASS |
| Batch ETL (60-day backfill, 296,817 records) | 9/9 reconciliation checks; injected = detected for all 8 defect kinds; HBase load verified |
| ML (synthetic labels) | accuracy 0.677, weighted F1 0.664, majority baseline 0.323; per-class F1 Low 0.875 · Moderate 0.541 · High 0.333 · Critical 0.787; save/reload verified |
| Live system | 4 events/s for >30 min; graceful stop 37 s; resume from checkpoints; recovery after a 75 s HBase outage |
| Dashboard | 7/7 tabs render project data in Edge; 0 console errors, 0 failed requests |
| Scripts | all 12 PowerShell entry points exit 0; cold start after `stop_all.ps1` → pipeline `ok` |

The generated dataset is reproducible: the same seed gives identical content
(only shifted in time) for any end date, so the ETL and ML numbers above are
expected to reproduce on another machine.

## 5. Issues found while running it (all fixed)

See the table "Problems found by running it, and fixed" in
[traceability.md](traceability.md): PyPI timeouts during the image build,
uncounted out-of-region citizen reports, live inference without incident data,
imputed "rainy" predictions for empty windows, a streaming JVM OOM kill, an
over-long graceful stop, inflated rate metrics, colliding live/hourly HBase
rows, HBase aborting after a long host pause, and restart exhaustion.

## 6. Known limitations

Synthetic data only; single-node deployment; at-least-once delivery;
watermark-late events do not update live windows; ~20 s streaming latency on
the Windows bind mount; ML "High" class recall 0.28; ~8 GB Docker memory needed
for the full stack plus batch jobs (with less, stop `streaming`/`producers`
while running batch jobs). HBase rows written before 2026-10-21 14:15 UTC on
the project machine use the old zone-risk key format and are ignored by the API
(a fresh machine does not have them).

## 7. Notes for development

* All tunables: `config/aegis.yaml`; overrides via `AEGIS__SECTION__KEY` (see `.env.example`).
* Spark collects timestamps in the driver's local time zone; code that turns
  Spark rows into JSON renders timestamps as UTC strings inside Spark
  (`hbase_sink.iso`). Keep that pattern.
* Native Windows Spark cannot write files without Hadoop `winutils`; run
  file-writing jobs in the container.
* `run_e2e.ps1` shares checkpoints with the `streaming` service; the script
  pauses and restores the live services itself.
