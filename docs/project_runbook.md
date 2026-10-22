# AEGIS — runbook (Windows PowerShell)

Copy-paste commands for every task. Run them from the project root:

```powershell
cd "D:\SEM 5\AEGIS"
```

If PowerShell blocks scripts, either run once
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, or prefix each script
with `powershell -ExecutionPolicy Bypass -File`.

## 0. Prerequisites (verified versions on the project machine)

| Need | Version used | Check |
|---|---|---|
| Docker Desktop (WSL 2) | engine 29.8.0, ≥ 8 GB memory for the full stack, ~10 GB free disk for its image | `docker version` |
| Node.js | 24.12 (18+ works) | `node --version` |
| Python on the host (optional) | 3.11.9 | `python --version` |
| Internet | first build, map tiles, real-data import, diagram rendering | — |

Inside the containers: Python 3.11, OpenJDK 17, PySpark 3.5.9, Kafka 4.3.1,
HBase 2.5.16. The host's Java 24 is not used (Spark 3.5 needs Java 17).

## 1. One-time setup

```powershell
scripts\setup.ps1 -HostVenv          # builds aegis-spark + aegis-hbase images, npm install, creates .venv
.\.venv\Scripts\Activate.ps1         # optional: activate the host venv in this terminal
.\.venv\Scripts\python.exe -m pip install playwright==1.55.0   # optional: browser UI check + diagram rendering
```

Success: "Setup complete. Next: scripts\start_infrastructure.ps1".
Configuration needs no changes; overrides go in environment variables named
`AEGIS__SECTION__KEY` (see `.env.example`). Docker Compose already sets the
container values. There are no secrets.

---

## 2. Basic demo (batch + dashboard, no live stream)

| Step | Command | Success looks like |
|---|---|---|
| 1 | `scripts\start_infrastructure.ps1` | Kafka and HBase `(healthy)`; topic JSON shows `created`/`exists`; 5 HBase tables listed |
| 2 | `scripts\generate_data.ps1` | `data\generated\historical\manifest.json`; line counts weather 69,600, water 46,375, traffic 173,926, incidents 6,916 |
| 3 | `scripts\run_batch_analytics.ps1 -Only etl` | JSON with all `reconciliation` checks `true`; `injected_vs_detected` equal for all 8 kinds; exit 0 |
| 4 | `scripts\train_model.ps1` | accuracy ≈ 0.677 printed; `data\models\flood_severity_dt\latest.json` |
| 5 | `scripts\start_api.ps1` | http://localhost:8000/health → `"status": "ok"`; Swagger at http://localhost:8000/docs |
| 6 | `scripts\start_frontend.ps1` (second terminal, keep open) | http://127.0.0.1:5173 shows the dashboard with a blue "synthetic" banner |

Without the stream, zones are served from the batch gold table
(`data_origin = batch-historical`, shown as stale/historical on the map).

## 3. Complete live pipeline (adds Kafka producers + Structured Streaming)

| Step | Command | Success looks like |
|---|---|---|
| 1–4 | as in § 2 (run `scripts\run_batch_analytics.ps1` without `-Only` to include RDD, SQL, spatial and risk demos and the HBase load) | — |
| 5 | `scripts\start_streaming.ps1` | `Get-Content data\state\streaming\status.json` → `"state": "running"`, `"hbase_sink": "enabled (hbase:9090)"`; Spark UI http://localhost:4040 → Structured Streaming tab shows `bronze_dq`, `silver`, `zone_risk` |
| 6 | `scripts\start_producers.ps1` | `docker compose logs --tail 5 producers` → `stats sent=… delivered=… failed=0` |
| 7 | `scripts\start_api.ps1` | as above |
| 8 | `scripts\start_frontend.ps1` (second terminal) | header "live updates connected"; zones turn solid (live) within a few minutes |
| 9 | after ~5 minutes: `scripts\train_model.ps1 -Infer` | "zones_with_prediction" ≈ 39 of 40 (zones without a weather observation get "Insufficient data") |

Useful variants:

```powershell
scripts\start_streaming.ps1 -Follow                 # show streaming logs
scripts\start_streaming.ps1 -Once                   # drain Kafka once (availableNow), then stop
scripts\start_producers.ps1 -Source weather -Rate 1 -Duration 60   # one source, foreground
docker compose run --rm --no-deps tools python -m producers.kafka_admin describe       # offsets per topic
docker compose run --rm tools python -m producers.kafka_consumer_check --timeout 10 --max 200
docker compose run --rm --no-deps tools python hbase/verify_hbase.py                   # live HBase checks
```

## 4. Optional analytics, ML and data modes

| Task | Command | Output |
|---|---|---|
| RDD demo | `scripts\run_batch_analytics.ps1 -Only rdd` | `data\reports\rdd_demo.json` (20 operations, RDD = DataFrame check) |
| Spark SQL | `scripts\run_batch_analytics.ps1 -Only sql` | `data\reports\sql\q1…q10.json` |
| Spatial | `scripts\run_batch_analytics.ps1 -Only spatial` | `data\reports\spatial_report.json` |
| Risk ranking | `scripts\run_batch_analytics.ps1 -Only risk` | console table |
| One ML prediction | `docker compose run --rm --no-deps tools python -m spark_jobs.model_inference --example` | JSON with `predicted_severity` and probabilities |
| Real historical data | `scripts\ingest_real_data.ps1` then `scripts\run_real_data.ps1`, then `scripts\start_frontend.ps1 -RealData` | API :8001, dashboard http://127.0.0.1:5174 (green banner) |
| Hybrid (real rain + synthetic fill) | `scripts\run_hybrid_data.ps1`, then `scripts\start_frontend.ps1 -Hybrid` | API :8002, dashboard http://127.0.0.1:5175 (amber banner) |
| Lake compaction (after long runs) | `docker compose stop streaming`; `docker compose run --rm --no-deps tools python -m spark_jobs.compact_lake`; `docker compose start streaming` | fewer, larger Parquet files |
| Re-render diagrams | `.\.venv\Scripts\python.exe scripts\render_diagrams.py` | `docs\*.svg`, `docs\*.png` (needs Playwright + internet) |

## 5. Health checks

```powershell
docker compose ps                                        # containers and health
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/api/pipeline/status | Select-Object overall, issues
Get-Content data\state\streaming\status.json | Select-Object -First 20
```

Browser: http://127.0.0.1:5173/#pipeline (services, queries, offsets, data quality),
http://localhost:16010 (HBase master), http://localhost:4040 (Spark UI).

## 6. Tests

```powershell
scripts\run_tests.ps1                                  # host unit tests, then container unit + Spark, then integration
.\.venv\Scripts\python.exe -m pytest tests/unit -q     # host only: 90 tests
docker compose run --rm tools python -m pytest tests -q -rs     # everything in the container: 138 tests
scripts\run_e2e.ps1                                    # 14-step end-to-end check (~15 min) -> data\reports\e2e_report.md
.\.venv\Scripts\python.exe scripts\ui_check.py         # every dashboard tab in Edge -> data\reports\ui\*.png
```

Success: "All selected test suites passed"; e2e summary `{'PASS': 14, 'FAIL': 0, …}`;
UI check `{"ok": true, "console_errors": 0, "failed_requests": 0}`.
Integration tests skip with a printed reason when Kafka/HBase are unreachable;
a skip is never counted as a pass.

## 7. Shutdown and cleanup

```powershell
scripts\stop_all.ps1                     # graceful stop; streaming finishes its micro-batch (≤ 90 s)
scripts\stop_all.ps1 -Down               # also remove containers (Kafka/HBase volumes kept)
docker compose --profile real --profile hybrid stop api-real api-hybrid    # the historical-mode APIs
```

`scripts\stop_all.ps1 -Down -DeleteVolumes` also **deletes Kafka messages and
HBase tables** — only use it to start from scratch. Local files in `data\`,
`data_real\` and `data_hybrid\` are never deleted by these scripts.

## 8. Without Docker

| Works on the host (`.venv`) | Needs Docker |
|---|---|
| 90 unit tests (`pytest tests/unit`) | every Spark job (Java 17), Kafka, HBase |
| real-data import: `scripts\ingest_real_data.ps1 -OnHost` | streaming, batch ETL, model training |
| API without model hosting: `scripts\start_api.ps1 -OnHost` (reads existing lake files) | model predictions in the API |
| dashboard: `scripts\start_frontend.ps1` | live updates (need Kafka + streaming) |
| producers against an existing broker: `scripts\start_producers.ps1 -OnHost` | — |

## 9. Troubleshooting (short)

| Symptom | Fix |
|---|---|
| `docker` hangs | start Docker Desktop; check free space on the drive holding its disk image |
| `topics missing on broker` | `docker compose run --rm --no-deps tools python -m producers.kafka_admin create` |
| `HBase Thrift server not reachable` | HBase needs ~1 min to start: `docker compose logs hbase` |
| streaming says `hbase_sink: disabled: unreachable at startup` | start HBase, then `docker compose restart streaming` |
| dashboard "Cannot reach the AEGIS API" | `scripts\start_api.ps1`; check `VITE_API_BASE_URL` |
| `/api/model/predict` → 503 | `scripts\train_model.ps1`; the host-run API cannot host the model |
| Overview slow after hours of streaming | compaction (§ 4) |
| HBase container exits after a long host pause | give Docker more memory or stop `streaming`/`producers` during batch jobs; it restarts automatically |

More: README "Troubleshooting".
