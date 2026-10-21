# AEGIS — faculty demonstration guide (~20 minutes)

Run everything from the project root in **PowerShell**. Steps 0–1 can be done
before the presentation.

## 0. Before the session (once)

```powershell
cd "D:\SEM 5\AEGIS"
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1          # builds images, installs dashboard
powershell -ExecutionPolicy Bypass -File scripts\start_infrastructure.ps1
powershell -ExecutionPolicy Bypass -File scripts\generate_data.ps1  # synthetic 60-day backfill
powershell -ExecutionPolicy Bypass -File scripts\run_batch_analytics.ps1 -Only etl
powershell -ExecutionPolicy Bypass -File scripts\train_model.ps1
```

## 1. Start the live system (terminal 1)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_streaming.ps1   # Spark UI: http://localhost:4040
powershell -ExecutionPolicy Bypass -File scripts\start_producers.ps1   # 4 synthetic events/s
powershell -ExecutionPolicy Bypass -File scripts\start_api.ps1         # http://localhost:8000/docs
```

Terminal 2 (keep open): `powershell -ExecutionPolicy Bypass -File scripts\start_frontend.ps1` → http://127.0.0.1:5173

Each dashboard tab has its own link, handy for jumping during the talk:
`#map`, `#zones`, `#incidents`, `#analytics`, `#model`, `#pipeline`
(e.g. http://127.0.0.1:5173/#pipeline). Let the producers run for ~5 minutes
before step 4 so live windows and alerts exist; a storm cell crosses the region
about every 20 minutes.

## 2. Show the problem and the data (2 min)

* Open `docs/architecture.md` diagram; point out the four topics and the two
  weather vendor formats.
* `Get-Content data\generated\historical\weather.jsonl -TotalCount 3` and
  `Select-String -Path data\generated\historical\weather.jsonl -Pattern '"wx-imperial-2"' | Select-Object -First 1` —
  same measurement, different keys/units/timestamps; all marked `"synthetic": true`.

## 3. Kafka ingestion (2 min)

```powershell
docker compose run --rm --no-deps tools python -m producers.kafka_admin describe
docker compose run --rm tools python -m producers.kafka_consumer_check --timeout 10 --max 200
docker compose logs --tail 5 producers
```
Show offsets growing, per-topic counts and producer delivery stats (acks=all, idempotent).

## 4. Structured Streaming (3 min)

* Spark UI → *Structured Streaming* tab: three queries (`bronze_dq`, `silver`, `zone_risk`), input/processing rates.
* `Get-Content data\state\streaming\status.json` → watermark, state rows, rows dropped by watermark (late events).
  The dashboard's "records" column is the exact Kafka record count per micro-batch
  (from offset ranges); "scanned" is Spark's per-branch scan count.
* Dashboard → **Pipeline**: services, queries, producers, Kafka offsets, live data quality, quarantined payloads.

## 5. ETL and data quality (3 min)

* `Get-Content data\reports\dq_report_batch.json` (or Dashboard → Pipeline → Data quality):
  records = valid + rejected, duplicates removed, **injected vs detected defects identical**.
* Explain bronze → silver → gold and the quarantine reasons.

## 6. Spark RDD and Spark SQL (3 min)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_batch_analytics.ps1 -Only rdd   # transformations vs actions, lineage, RDD = DataFrame check
powershell -ExecutionPolicy Bypass -File scripts\run_batch_analytics.ps1 -Only sql   # 10 SQL queries: joins, LAG, RANK, moving average
```

## 7. HBase wide-column store (2 min)

```powershell
docker compose run --rm --no-deps tools python hbase/verify_hbase.py   # put/get/versions/reverse scan/delete
```
Open http://localhost:16010 (tables). Dashboard → **Zone risk** → a zone: the
history comes from `zone_risk_history` (reverse prefix scan) and the cell
versions show how one window's score evolved.

## 8. Risk analytics and spatial analysis (3 min)

* Dashboard → **Live map**: zones coloured by rule-based risk, live reports;
  click the map → nearby hazards within the radius (Haversine).
* **Zone risk** → contributions (points per indicator), coverage/confidence,
  missing indicators ("missing ≠ safe"), station assignment.
* `powershell -ExecutionPolicy Bypass -File scripts\run_batch_analytics.ps1 -Only spatial`.
* **Historical analytics**: daily incidents, rain vs river level, risk trend, rain band vs disruption.

## 9. Spark MLlib model (2 min)

* `Get-Content data\reports\ml_evaluation.json` or Dashboard → **ML model**:
  synthetic-label banner, time-based split, held-out metrics vs majority
  baseline, confusion matrix, feature importances.
* Prefill the form from a zone → **Predict** → compare with that zone's
  rule-based score (they are independent).
* `powershell -ExecutionPolicy Bypass -File scripts\train_model.ps1 -Infer` → per-zone predictions (Parquet + HBase).

## 10. Failure handling (optional, 2 min)

* `docker compose stop hbase` → within ~30 s **Pipeline** shows `degraded` /
  "HBase unreachable", and **Zone risk** is served from the streaming snapshot
  (`read_from = snapshot`). The affected streaming queries fail and restart from
  their checkpoints (restart counter 1, back-off 10 s).
  `docker compose start hbase` → after ~1 minute everything is `ok` again and
  zones are read from HBase. (Verified on 2026-10-21 with a 75 s outage.)
* `docker compose restart streaming` → the queries stop after their in-flight
  batches and resume from the checkpoints (batch ids continue).
* Watch the alerts feed when a storm cell passes (every ~20 min with default settings).

## 11. Tests and end-to-end check

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_tests.ps1
powershell -ExecutionPolicy Bypass -File scripts\run_e2e.ps1   # 14 steps -> data\reports\e2e_report.md (~10 min; pauses/restores streaming+producers)
.\.venv\Scripts\python.exe scripts\ui_check.py                  # every dashboard tab in a real browser -> data\reports\ui\*.png
```

## Stop

```powershell
powershell -ExecutionPolicy Bypass -File scripts\stop_all.ps1
```
