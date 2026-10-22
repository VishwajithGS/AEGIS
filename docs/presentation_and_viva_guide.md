# AEGIS — presentation and viva guide

Everything here matches the code and the measured runs (2026-10-21/22). Use
the slides in [AEGIS_architecture.pptx](AEGIS_architecture.pptx) while
speaking; details are in [project_codebase_guide.md](project_codebase_guide.md).

---

## A. Project overview

**Problem.** In an urban flood, the useful signals — rainfall, river levels,
traffic, citizen reports — come from different systems, in different formats
and units, continuously, and some of them are late, duplicated, wrong or
missing. Decision makers need one consistent, explainable view of where risk
is building up.

**Motivation.** Chennai floods repeatedly (2015, Cyclone Michaung in December
2023). The data-engineering part of the problem — combining messy,
high-volume, multi-source streams in real time — is exactly what Big Data
tools are designed for.

**Objectives.**
1. Ingest multiple semi-structured, time-series streams through Kafka.
2. Validate, map, standardise, de-duplicate and quarantine with Spark (batch and streaming).
3. Use Spark RDDs, Spark SQL, Structured Streaming and MLlib.
4. Compute explainable zone risk and spatial analytics; store in HBase and Parquet.
5. Serve live and historical results on a dashboard; verify everything with tests.

**Solution.** A Kafka → Spark → HBase/Parquet → FastAPI → React pipeline over
a 40-zone grid of Chennai, with a transparent risk index and a supporting ML
classifier.

**Main contributions.**
* One set of validation/mapping rules shared by batch and streaming (6 source formats, 12 rejection codes).
* Data-quality reconciliation: every deliberately injected defect is detected (8/8 kinds, counts equal).
* An explainable risk score that treats missing data as *missing*, not safe.
* Wide-column HBase design with time-ordered row keys and versioned risk cells.
* Honest data modes: synthetic, real historical (ERA5, IMD, NOAA, DFO, GDACS) and hybrid, always labelled.
* 138 automated tests and a 14-step end-to-end check.

**Scope and limitations (say these yourself before you are asked).**
* The live stream is synthetic; real data is historical batch only.
* Single machine: one Kafka broker, standalone HBase, Spark local mode.
* Risk weights are demonstration values; ML labels are synthetic.

---

## B. Architecture talk (about 4 minutes)

*(Slide 2 — layered architecture)*

"AEGIS has six layers, and data flows top to bottom.

**Sources.** In the default mode a simulator generates a consistent synthetic
world over Chennai: moving rain cells, rivers that rise with accumulated rain,
traffic that slows down, and incidents that people report. It produces six
deliberately different formats — Celsius and Fahrenheit, millimetres and
inches, three timestamp styles — and injects defects like duplicates, broken
JSON and impossible values, so the pipeline has real cleaning work to do. We
also have a real-data mode with ERA5, IMD and NOAA rainfall and official
flood events for Cyclone Michaung.

**Ingestion.** Producers publish to four Kafka topics, one per source, keyed
by station id so each station's events stay in order. Kafka decouples the
sensors from processing: if Spark is slow, events wait in the log instead of
being lost.

**Processing.** Spark Structured Streaming reads the topics every ten seconds
with three queries. The first archives every raw record and measures data
quality. The second validates, de-duplicates within a ten-minute watermark and
writes clean records. The third assigns every record to zones using the
nearest station, aggregates five-minute windows per zone and computes the risk
score. The batch ETL runs the *same* validation code over the history to build
the bronze, silver and gold layers, the ML training set and a data-quality
report.

**Storage.** HBase holds operational data the dashboard needs fast — the
latest risk of each zone, its history and versions — using time-ordered row
keys. Parquet holds the history for analytics and ML. Two derived Kafka topics
carry risk updates and quality metrics onward.

**Serving.** A FastAPI backend reads HBase, Parquet and the derived topics and
exposes REST endpoints and a Server-Sent Events stream.

**Presentation.** The React dashboard shows a Leaflet map with risk-coloured
zones, explanations of each score, historical charts, the ML model and the
pipeline's health — and it always says whether a number is live, historical,
synthetic or real."

*(Slides 3–4 — one event and the batch/ML flow, if time allows.)*

---

## C. Feature by feature

| Feature | What / why | How (code) | Show live |
|---|---|---|---|
| Heterogeneous synthetic sources | realistic mapping work; reproducible (seed 42) | `producers/world.py`, `formats.py`, `anomalies.py` | `Get-Content data\generated\historical\weather.jsonl -TotalCount 2` (two formats) |
| Kafka ingestion | decoupling, ordering per key, replay | `producers/kafka_client.py` (`acks=all`, idempotent, dead-letter) | `kafka_admin describe` → offsets growing |
| Validation + mapping | one set of rules for batch and stream | `spark_jobs/transforms.canonicalize` | quarantine sample in the Pipeline tab |
| Structured Streaming | event-time windows, watermark, dedup, checkpoints | `spark_jobs/streaming_pipeline.py` | Spark UI :4040; `status.json` (watermark, dropped rows) |
| Batch ETL + lake | reproducible history, gold tables | `spark_jobs/etl_pipeline.py` | `dq_report_batch.json` |
| DQ reconciliation | proves every defect is caught | `dq_report()` | injected = detected table |
| HBase | fast keyed reads, versions, TTL | `aegis/storage/hbase_repository.py`, `hbase_sink.py` | `hbase/verify_hbase.py`; Zone tab → cell versions |
| Risk scoring | explainable, missing ≠ safe | `aegis/risk.py` | Zone tab → contributions and explanation |
| Spatial analytics | zone assignment, nearby search | `aegis/geo.py`, `spatial_analytics.py` | click the map → nearby hazards |
| RDD demo | transformations vs actions, lineage | `spark_jobs/rdd_demo.py` | `run_batch_analytics.ps1 -Only rdd` |
| Spark SQL | 10 analytical queries | `spark_jobs/sql_analytics.py` | `-Only sql` |
| MLlib model | supporting severity classifier | `model_training.py`, `model_inference.py` | ML tab → metrics, confusion matrix, prediction form |
| API | one place to read everything | `backend/app/api/routes.py` | http://localhost:8000/docs |
| Dashboard + live updates | push updates, alerts | `useLiveStream.js`, `RiskMap.jsx` | Live map; Overview alerts |
| Fault handling | restarts, fallbacks, graceful stop | streaming back-off, API fallbacks | `docker compose stop hbase` (optional) |
| Real / hybrid modes | real data where it exists, labelled | `aegis/realdata/`, `producers/hybrid_generator.py` | dashboards on 5174 / 5175 |

---

## D. Technology justification (short answers)

* **Kafka, not RabbitMQ/REST/DB inserts**: a durable, replayable, partitioned log; Spark reads it with checkpointed offsets; producers never wait for processing.
* **Spark, not pandas**: event-time windows, watermarks, fault-tolerant state, one API for batch and streaming, MLlib — and the same code would run on a cluster.
* **HBase, not PostgreSQL/MongoDB**: the course's wide-column model; row-key range scans for time series, cell versions and TTLs. PostgreSQL would work at this size; we say so.
* **Parquet, not CSV**: columnar, compressed, typed, partitioned; readable by Spark and by the API without Spark.
* **FastAPI**: validation from type hints, async Server-Sent Events, automatic API docs.
* **React + Leaflet**: interactive map without API keys; components for 7 tabs.
* **Docker Compose**: Kafka, HBase and Java 17 Spark on Windows with one command each.

Full matrix: [technology_stack_guide.md](technology_stack_guide.md).

---

## E. Viva questions and answers

### Overall architecture

1. **Why so many components for a small demo?** Each layer does a different job (buffering, computation, fast lookups, history, serving, UI). The course objectives require Kafka, Spark, HBase and MLlib, and the design scales out without code changes. The actual scale is small (≈ 300 k records, 4 events/s) and we do not claim distributed performance.
2. **What is the single most important design decision?** Sharing `transforms.canonicalize()` between batch and streaming, so validation rules can never differ between the live and historical views.
3. **What happens if one component fails?** Kafka buffers when Spark is down; streaming restarts from checkpoints with back-off; the API falls back from HBase to a snapshot file and to the batch gold table and marks data as stale; the dashboard falls back from SSE to polling.
4. **Lambda or Kappa architecture?** Closer to Lambda: a streaming (speed) layer and a batch layer that share the same code; the batch ETL can also re-process the archived Kafka bronze, so the batch layer is the reproducible source of truth.
5. **Where is the "real-time" part exactly?** Producers → Kafka → Structured Streaming (10 s trigger) → HBase / `zone-risk-updates` → API SSE → browser. End-to-end latency is tens of seconds (micro-batches took 30–58 s on this Windows machine after a day of running).

### Kafka and event-driven systems

6. **What is a topic and a partition?** A topic is a named log; partitions split it for parallelism. Order is guaranteed within a partition.
7. **Why key messages by source id?** All events of one station go to the same partition, so they stay in order.
8. **What delivery guarantee do producers have?** At-least-once: `acks=all` plus retries; idempotence prevents duplicates caused by retries inside the broker. Undeliverable records go to a dead-letter file and can be replayed.
9. **How does Spark track what it has read?** In each query's checkpoint, not in a consumer group. On restart it continues from the stored offsets.
10. **Why is auto topic creation disabled?** So a typo can never silently create a new topic; topics are created explicitly by `kafka_admin create`.
11. **Follow-up: is Kafka fault tolerant here?** Not across brokers: one broker, replication factor 1. The data survives container restarts (volume) but not disk loss.

### Spark and distributed computing

12. **What is lazy evaluation?** Transformations only build a plan (lineage); nothing runs until an action such as `count` or a write.
13. **Driver vs executors?** The driver plans and coordinates; executors run tasks on partitions. In local mode both are threads in one JVM.
14. **Why `local[8]` and `local[4]`?** The whole stack must fit an 8 GB Docker VM; `local[*]` on 16 cores started 16 Python workers and the streaming JVM was killed for memory.
15. **What is a shuffle and where do you have one?** Data redistribution across partitions for grouping or joining — e.g. `reduceByKey`, `groupBy(window, zone_id)`, the dedup window.
16. **Why a Python UDF for the risk score?** One definition of the formula shared by Spark, the API and the tests. Cost: Python serialization; acceptable at this scale.

### RDDs versus DataFrames

17. **Difference?** RDDs are untyped collections of objects with hand-written logic; DataFrames have a schema and are optimised by Catalyst.
18. **Which operations does the RDD demo show?** `textFile, map, filter, flatMap, reduceByKey, aggregateByKey, mapValues, distinct, sortBy`; actions `count, first, take, collect, reduce`; plus `toDebugString` lineage.
19. **How do you know the RDD result is right?** The same category counts are computed with the DataFrame API and compared — identical.
20. **When would you still use RDDs?** Unstructured data or custom partition logic; otherwise DataFrames.

### Structured Streaming and event-time processing

21. **Event time vs processing time?** Event time is when the reading was taken (from the payload); processing time is when Spark sees it. Windows use event time.
22. **What are your three queries?** `bronze_dq` (archive + quality), `silver` (dedup + clean store), `zone_risk` (windows + score).
23. **Why `update` output mode for zone risk?** A window's result is re-emitted whenever new data changes it; HBase keeps 5 versions so the history of that window is visible.
24. **What is `foreachBatch` for?** It lets each micro-batch write to sinks Spark has no native connector for (HBase) and to several sinks at once.

### Watermarks and late data

25. **What is the watermark?** "Max event time seen − 10 minutes". Stateful operators drop events older than that and can clean their state.
26. **What happens to a late event?** If it is within the watermark it updates its window; if older, the dedup and window operators drop it (counted as "dropped by watermark") — but `bronze_dq` still archives it with a `late_arrival` flag, and the batch ETL includes it.
27. **Why 10 minutes?** A trade-off between completeness and state size; the generator sends late events both inside (3–8 min) and beyond (25–90 min) it to exercise both paths.
28. **How are duplicates removed in streaming?** `dropDuplicatesWithinWatermark(["event_id"])` — state is kept only within the watermark.

### ETL and Parquet

29. **Bronze, silver, gold?** Bronze = raw as received; silver = validated, standardised, de-duplicated; gold = aggregated, analytics-ready.
30. **How do you prove the ETL is correct?** Reconciliation: records = valid + rejected, valid − duplicates = silver rows, raw = parsed + unknown-topic, injected = detected per defect kind, unique ids, risk within [0, 100]. All passed on 296,817 records.
31. **Why Parquet?** Columnar, compressed, typed, partitioned by date; read by Spark and by pyarrow in the API.
32. **How is a wrong type handled?** Every field is parsed as text and cast explicitly; a failed cast gives `invalid_type:<field>`, so the record is rejected with a precise reason instead of being unreadable.

### HBase and row-key design

33. **Why HBase?** Fast access by key and key range, versioned cells, TTLs, sparse rows — the wide-column model the course asks for.
34. **Explain the `zone_risk_history` key.** `zone_id#series#window_start_ms` with a 13-digit timestamp: one zone's windows are contiguous and in time order; `series` (`live`/`hourly`) keeps batch and streaming rows apart.
35. **How do you get the latest risk of a zone?** A reverse scan with prefix `Z-R04-C02#live#`, limit 1.
36. **Why a reverse timestamp for predictions?** `9999999999999 − ts` makes a forward scan return the newest first.
37. **Are writes idempotent?** Yes: the same event always maps to the same row key, so a replayed micro-batch overwrites identical cells.
38. **Follow-up: weaknesses?** No secondary indexes or joins, complex operations (JVM, ZooKeeper, Thrift), standalone mode here. PostgreSQL would serve 40 zones easily.

### Spatial analytics

39. **How is a point assigned to a zone?** `row = floor((lat − 12.85)/0.05)`, `col = floor((lon − 80.10)/0.05)` → `Z-R<row>-C<col>`; outside the box → rejected. Example (13.05, 80.22) → Z-R04-C02.
40. **Haversine formula?** `a = sin²(Δφ/2) + cos φ1 cos φ2 sin²(Δλ/2)`, `d = 2R asin(√a)`, R = 6371.0088 km.
41. **Why nearest station within a radius?** A zone takes its rainfall from the nearest station within 12 km and its river level from the nearest gauge within 6 km; beyond that the value is *not observed* rather than borrowed from far away (19 of 40 zones have a gauge).
42. **Limitations?** Regular grid, not administrative boundaries; spherical Earth; no road network or elevation.

### Risk scoring

43. **State the formula.** `risk = 100 · Σ w_i f_i / Σ w_i` over available indicators; `f_i` is a clamped linear ramp between `low` and `high`.
44. **Weights?** Rainfall 0.30, water level 0.25, incidents 0.20, traffic 0.15, historical 0.10.
45. **Why exclude missing indicators instead of using 0?** A missing gauge reading is not evidence of safety. Missing weight lowers *coverage* and confidence; below 40 % coverage the zone is "Insufficient data".
46. **Worked example?** Rain 20 mm/hr, level 0.9, 2.5 weighted reports, 50 % disrupted, hazard 0.6 → 11.05 + 18.75 + 10 + 10 + 6 = 55.8 → High.
47. **Does heavy rain mean a flood?** No. Rain is one input (30 %); flooding depends on accumulation, rivers and drainage. Rain alone gives "Insufficient data".
48. **Are the weights validated?** No — they are documented demonstration values in `config/aegis.yaml`.

### Machine learning and evaluation

49. **What does the model predict?** Severity (Low/Moderate/High/Critical) of a flood-related incident from conditions before it.
50. **Where do the labels come from?** A documented synthetic rule in the generator. They are not real outcomes.
51. **How do you avoid leakage?** Features only use data from buckets before the incident; forbidden columns are rejected; the imputer is fitted on the training split only; the split is time-based.
52. **Results?** Accuracy 0.677, weighted F1 0.664, majority baseline 0.323 (365 test incidents). "High" is weakest (F1 0.333).
53. **What do the metrics mean?** How well a depth-6 decision tree recovers the generator's rule from noisy sensor features — not real-world skill.
54. **Why a decision tree?** Interpretable (feature importances, readable splits), handles mixed scales without normalisation, fast. Top feature: `rain_3h_mm` (0.71).
55. **Follow-up: why not train on real data?** No verified open source gives incident-level severity for Chennai; DFO/GDACS severities describe whole regional events. In real mode no model is served; a defensible proxy target (IMD next-day rainfall class) is documented but not implemented.
56. **Is the ML prediction the risk score?** No. Two independent outputs; the score is a formula, the prediction is a learned estimate.

### FastAPI and REST

57. **Why Server-Sent Events, not WebSockets?** Updates flow one way (server → browser); SSE is plain HTTP, reconnects automatically and is simpler.
58. **How are inputs validated?** Pydantic models with bounds and `extra="forbid"`; query parameters with patterns and ranges → 422 on errors; 503 if the model is unavailable.
59. **How does the API avoid showing old data as live?** Each zone carries `data_origin` and `stale` (window ended more than 15 minutes ago).

### React and Leaflet

60. **How does the map update live?** SSE `zone_risk` event → 3-second debounce → reload `/api/zones` → rectangles recoloured.
61. **What does a dashed zone mean?** Stale or historical assessment.

### Docker and deployment

62. **Why Docker?** Kafka, HBase and Java 17 Spark on Windows, reproducibly, one command each; the host's Java 24 is unsupported by Spark 3.5.
63. **How do containers find each other?** By service name on the Compose network (`kafka:29092`, `hbase:9090`); the host uses `localhost` ports.

### Data quality, fault tolerance, testing, limitations

64. **How many tests?** 138 in the container (90 unit, 43 Spark, 5 integration against live Kafka/HBase/Spark) + a 14-step end-to-end check (14/14 PASS) + a browser check of all 7 tabs.
65. **Exactly-once?** Not claimed. Offsets are checkpointed, HBase writes are idempotent, Parquet appends are guarded by batch markers — at-least-once with idempotent sinks.
66. **Biggest limitation?** The live data is synthetic; real data is historical and lacks water level, traffic and reports, so the real-only risk is "Insufficient data" (hybrid mode fills the gaps with labelled synthetic data).
67. **A problem you found by running it?** E.g. batch and live risk rows overwrote each other in HBase (same key) → separate `live`/`hourly` key series; or the API became slow after ~10,000 small streaming files → faster file listing and cached row counts.

---

## F. Ten-minute live demo

Before the session (≥ 15 minutes earlier): run §§ 2–3 of
[project_runbook.md](project_runbook.md) so infrastructure, batch layer,
model, streaming, producers, API and dashboard are running; let producers run
≥ 5 minutes.

| Min | Show | Command / page | Evidence |
|---|---|---|---|
| 0–1 | the data | `Get-Content data\generated\historical\weather.jsonl -TotalCount 2` | two formats, `"synthetic": true` |
| 1–2 | Kafka | `docker compose run --rm --no-deps tools python -m producers.kafka_admin describe` | 6 topics × 3 partitions, growing offsets |
| 2–4 | Streaming | http://localhost:4040 (Structured Streaming tab); `Get-Content data\state\streaming\status.json` | 3 active queries, watermark, dropped-by-watermark |
| 4–5 | Data quality | Dashboard → Pipeline | quarantine reasons, injected = detected |
| 5–7 | Risk + map | Dashboard → Live map, click a zone → Zone risk | coloured zones, contributions, explanation, HBase history + versions |
| 7–8 | Nearby search | click the map | hazards within 2 km, Haversine distances |
| 8–9 | ML | Dashboard → ML model, "Predict" | metrics, confusion matrix, synthetic-label banner |
| 9–10 | Real/hybrid | http://127.0.0.1:5175 (hybrid) | amber banner, cyclone peak 4 Dec 2023 in Historical analytics |

**Fallbacks.**
* Kafka or streaming fails: show the batch layer — Historical analytics,
  `data\reports\dq_report_batch.json`, `data\reports\e2e_report.md`.
* HBase fails: the dashboard keeps working from snapshot/gold (point at
  `read_from` and `source_errors`); explain the fallback design.
* Docker fails: show the screenshots in `data\reports\ui\`, the reports in
  `data\reports\`, and run `.\.venv\Scripts\python.exe -m pytest tests/unit -q`
  on the host (90 tests).
* Never claim something works that you did not just show; refer to the
  dated evidence in `docs/traceability.md` instead.

---

## G. Limitations and future improvements

| Limitation | Improvement |
|---|---|
| Live stream is synthetic | connect NDMA SACHET live warnings (verified, not built) and a real rain-gauge feed when one is accessible |
| Real mode lacks water level, traffic, reports | GloFAS river discharge as a documented proxy; CWC data if an API becomes available |
| ML labels are synthetic | train on a real proxy target (IMD next-day rainfall class) over several monsoon seasons |
| Single node | multi-broker Kafka with replication 3, HBase on HDFS, Spark on a cluster; measure throughput |
| At-least-once delivery | transactional Kafka sinks or Delta Lake for exactly-once batch outputs |
| Small files from streaming | scheduled compaction or a table format (Delta/Iceberg) |
| No authentication | add auth and TLS before any non-local deployment |
| Risk weights not validated | calibrate against historical flood records (e.g. Global Flood Database) |
