# AEGIS architecture

AEGIS follows the faculty reference architecture (ingest → stream processing →
cold storage / analytical store → ML, monitoring, dashboards) with concrete
technologies at every stage.

```
 Synthetic sources (Python)                    Kafka (KRaft, 1 broker)            Spark 3.5.9 (PySpark)
 ┌──────────────────────────┐   JSON events   ┌──────────────────────┐
 │ weather (2 vendor formats)│ ─────────────▶ │ weather-events       │──┐
 │ river gauges (m/cm/ft)    │ ─────────────▶ │ water-level-events   │──┤   Structured Streaming
 │ traffic segments          │ ─────────────▶ │ traffic-events       │──┼─▶ ① bronze_dq ─▶ bronze/kafka_raw, quarantine/stream,
 │ citizen reports           │ ─────────────▶ │ incident-reports     │──┘                  live/dq_metrics ─▶ data-quality-events
 └──────────────────────────┘                 │ zone-risk-updates  ◀─┼──── ② silver ────▶ silver_stream/<domain> + HBase
 historical backfill (JSONL, 60 days) ───┐    │ data-quality-events◀─┼──── ③ zone_risk ─▶ live/zone_risk + HBase zone_risk_history
                                         │    └──────────────────────┘
                                         ▼
                               Batch ETL (Spark): bronze/files ─▶ silver/<domain> ─▶ gold/* ─▶ HBase (optional load)
                               RDD demo · Spark SQL · spatial · risk ranking · MLlib training/inference
                                         │
                Parquet lake (data/lake) + HBase (wide-column, Thrift) + state JSON (data/state)
                                         │
                                FastAPI (pyarrow reads, happybase reads, Kafka live feed, Spark MLlib model)
                                         │  REST + Server-Sent Events
                                React + Vite + Leaflet dashboard
```

## Components and processes

| Process | Where it runs | Code | Purpose |
|---|---|---|---|
| Kafka broker | `kafka` container (apache/kafka 4.3.1, KRaft) | `docker-compose.yml` | event log; host listener `localhost:9092`, internal `kafka:29092` |
| HBase | `hbase` container (HBase 2.5.16 standalone + Thrift) | `docker/hbase/` | wide-column operational store; Thrift `9090`, UI `16010` |
| Producers | `producers` container or host venv | `producers/` | synthetic live events, paced, with injected defects |
| Streaming job | `streaming` container (Spark local[4]) | `spark_jobs/streaming_pipeline.py` | three Structured Streaming queries |
| Batch jobs | `tools` container | `spark_jobs/*.py` | ETL, RDD, SQL, spatial, risk, ML |
| API | `api` container | `backend/app/` | REST, SSE, MLlib inference |
| Dashboard | host (Node) | `frontend/` | React/Leaflet UI |

All Python processes share the `aegis` package (configuration, geography,
risk formula, canonical vocabularies, HBase repository) and one configuration
file, `config/aegis.yaml`, overridable with `AEGIS__SECTION__KEY` variables.

## Data sources

AEGIS has three data roots, each labelled: `data/` (synthetic, default,
described here), `data_real/` (real historical, see
[Real historical data mode](#real-historical-data-mode) below) and
`data_hybrid/` (real rainfall + synthetic fill, see [Hybrid mode](#hybrid-mode)).

In synthetic mode all sources come from the simulator (see `producers/world.py`): a moving-storm rain
field, river levels driven by accumulated rain, traffic slowed by rain and
flooding, and incidents sampled from hazard-dependent rates. Every record
carries `"synthetic": true`. Heterogeneity is deliberate:

| Topic | Schema versions | Differences the ETL must reconcile |
|---|---|---|
| `weather-events` | `wx-metric-1`, `wx-imperial-2` | different keys (`event_id`/`obs_id`, `source_id`/`station`), nested location, °F and in/hr, epoch ms vs ISO+05:30, METAR codes |
| `water-level-events` | `wl-1` | level in m, cm or ft per sensor; free-text sensor status |
| `traffic-events` | `tr-1` | local time without offset (Asia/Kolkata), free-text disruption |
| `incident-reports` | `pr-1` (+ `hi-1` archive in files) | free-text categories, optional coordinates, verification states |

The historical backfill (60 days, ~297k raw records) is written as JSON-lines
files that stand in for cold storage. The generator records every injected
defect in `manifest.json` so the ETL's detection can be verified exactly.

### Real historical data mode

```
ERA5 (Open-Meteo) ┐  aegis.realdata.ingest
IMD gridded       │  validate · units · UTC · provenance
NOAA GHCN-D       ├─▶ deterministic ids · SHA-256 cache ─▶ data_real/raw/historical
DFO archive       │                                              │
GDACS             ┘                                              ├─▶ etl_pipeline (same code) ─▶ silver/weather ─▶ gold zone_hourly, zone_risk_hourly
                                                                 └─▶ real_data_layers ─▶ gold real_daily_rainfall, station_daily_rainfall,
                                                                                          official_events + reports/real_data_validation.json
data_real/lake ─▶ api-real (:8001; HBase, live feed and model off) ─▶ dashboard (:5174)
```

* ERA5 records are a new schema version (`wx-era5-1`) of the existing weather
  family, so `transforms.py`, the ETL, risk, SQL and RDD jobs process them
  unchanged; every canonical row now carries `data_source` and
  `observation_type`.
* Daily rainfall and official events are not Kafka topics; they only feed the
  batch real-data layers.
* Real mode is selected by environment overrides (`AEGIS__STORAGE__DATA_ROOT=data_real`,
  `AEGIS__RISK__REPORTS_OBSERVED=false`, `AEGIS__HBASE__ENABLED=false`); Kafka
  topics, HBase tables and the synthetic lake are not touched.
* No live stream in real mode. Replaying real history through Kafka is
  possible but not implemented (it would need a "historical replay" label).

### Hybrid mode

`producers/hybrid_generator.py` builds a third data root, `data_hybrid/`:
the real files of `data_real/` plus synthetic water level, traffic, reports
and incidents from the unchanged simulator, whose rain field
(`Environment.rain`) is replaced by the real ERA5 value of the zone and hour.
The same batch ETL, risk, real-data layers and MLlib jobs run on it, and
`api-hybrid` (:8002, HBase and live feed off, model on) serves it to the
dashboard (:5175). Provenance columns keep real and synthetic rows apart in
every silver table.

## Kafka design

* Keys = source id → per-source ordering within a partition; 3 partitions per topic.
* Values = UTF-8 JSON. Producer: `acks=all`, `enable.idempotence=true`,
  retries, `lz4` compression, delivery callbacks. Final delivery failures go to
  `data/dead_letter/producer_failures.jsonl` (replay: `producers/replay_dead_letter.py`).
* Retention: 7 days for source topics, 2 days for derived topics.
* Derived topics: `zone-risk-updates` (one JSON per updated zone window; consumed
  by the API live feed) and `data-quality-events` (one summary per micro-batch).

## ETL layers

| Layer | Location | Content |
|---|---|---|
| Bronze | `lake/bronze/kafka_raw` (stream), `lake/bronze/files` (batch) | raw payload string + topic/partition/offset/Kafka timestamp; nothing dropped |
| Quarantine | `lake/quarantine/{stream,batch}` | rejected records with `reasons` (e.g. `missing_required:event_time`) and the first 2,000 chars of the payload |
| Silver | `lake/silver/<domain>`, `lake/silver_stream/<domain>` | canonical schema, UTC `event_time`, metric units, normalised categories, `zone_id`, `dq_flags`; unique `event_id` |
| Gold | `lake/gold/*`, `lake/live/*` | zone-hour aggregates, risk history, incident trends, correlations, ML training set, DQ reports, live zone risk, ML predictions |

Validation (`spark_jobs/transforms.py`) parses every JSON leaf as a string and
casts explicitly, so a wrong type is reported as `invalid_type:<field>`
rather than making the whole record unreadable. Rules: malformed JSON, source
type vs topic, schema version, required fields, types, timestamp parse,
future timestamps (> 5 min ahead), coordinate validity, study-region bounds,
physical ranges (`aegis/canonical.py: RANGES`) and units. Non-fatal problems
become flags on valid records: `late_arrival`, `sensor_fault`,
`sensor_degraded`, `missing_coordinates`, `unknown_category`, `missing_optional`.

Event time and processing time are kept apart (`event_time`, `ingest_time`
= Kafka timestamp, `processing_time`). No measurement is ever invented: missing
optional values stay NULL; imputation happens only inside the ML pipeline.

## Structured Streaming

| Query | Stateful? | Output mode | Sinks | Checkpoint |
|---|---|---|---|---|
| `bronze_dq` | no | append (foreachBatch) | bronze, quarantine, `live/dq_metrics`, HBase `processing_metrics`, `data-quality-events` | `data/checkpoints/bronze_dq` |
| `silver` | dedup `dropDuplicatesWithinWatermark(event_id)` | append (foreachBatch) | `silver_stream/<domain>`, HBase `sensor_observations` / `incident_records` | `data/checkpoints/silver` |
| `zone_risk` | dedup + windowed aggregation | **update** (foreachBatch) | `live/zone_risk`, HBase `zone_risk_history`, `zone-risk-updates` | `data/checkpoints/zone_risk` |

* **Event-time windows:** tumbling `streaming.window_duration` (5 minutes) per zone.
* **Watermark:** `streaming.watermark` (10 minutes) behind the max event time.
  It bounds dedup and aggregation state. A record older than the watermark is
  **dropped by queries 2 and 3** (it never updates a live window) — this is
  intentional and reported as `dropped_by_watermark` in query progress. Query 1
  is stateless: it still archives such records in bronze and flags them
  `late_arrival` (ingest − event time > watermark), so the batch layer, which
  re-reads bronze, includes them in historical analytics.
* **Triggers:** processing time every 10 s, or `--available-now` to drain the
  topics and stop (used by tests and the end-to-end check).
* **Monitoring:** a `StreamingQueryListener` writes progress (Kafka records,
  rates, watermark, state rows, rows dropped by watermark) to
  `data/state/streaming/status.json` and `progress.jsonl`; the Spark UI is on
  port 4040. Record counts and rates come from the Kafka offset ranges of each
  micro-batch: Spark's own `numInputRows` counts the source once per plan branch
  that scans it (`silver` splits by domain, `zone_risk` also by signal), so it
  overstates ingest by ~4× / ~16×; it is kept as `rows_scanned`.
* **Resources:** the streaming driver runs `local[4]` with a 1 GB heap
  (`streaming.spark_master`, `streaming.driver_memory`). With `local[*]` on a
  16-core host, one Python UDF worker per core plus a 2 GB heap exceeded the
  3 GB container limit and the JVM was OOM-killed (found during verification).
* **Measured behaviour (this machine):** at 4 events/s each micro-batch takes
  ~12–22 s, mostly Parquet commits on the Windows bind mount; batch sizes stay
  stable, i.e. the job keeps up, with results ~20 s behind the events.
* **Restart / recovery:** offsets and state live in the checkpoints. A failed
  query is restarted from its checkpoint with back-off (10, 20, 40, 60, 60 s,
  max 5 attempts); the counter resets after 10 minutes of healthy running. A
  driver crash is recorded as `state: crashed` in the status file and Docker
  restarts the container (`restart: unless-stopped`), which resumes from the
  checkpoints. Verified: stopping HBase for ~75 s caused one restart per
  affected query and full recovery once HBase returned.
* **Graceful stop:** `Ctrl+C`/SIGTERM or the file `data/state/streaming/STOP`
  stops all queries concurrently, each after its in-flight micro-batch commits
  (verified: 37 s, inside the 90 s `stop_grace_period`, exit code 0).
* **Guarantees (what is actually implemented):** at-least-once from Kafka to
  every sink. HBase puts are idempotent by row key; Parquet appends are guarded
  by per-batch marker files (`<checkpoint>/_aegis_batches/<id>.done`), so a
  replayed batch is not appended twice unless the job dies between the write
  and the marker. Kafka outputs (`zone-risk-updates`) can contain repeats after
  a replay. **End-to-end exactly-once is not claimed.**
* **File sizes:** each micro-batch writes one file per partition (`coalesce(1)`);
  `spark_jobs/compact_lake.py` rewrites closed partitions into one file.

## Spatial model

* **Zones:** a regular 0.05° grid (≈5.5 km) over the demo bounding box
  12.85–13.25 °N, 80.10–80.35 °E → 8 × 5 = 40 zones, ids `Z-R<row>-C<col>`.
  A point belongs to the cell `floor((lat − min_lat) / cell)`; points on the
  north/east edge go to the last row/column; points outside have no zone and
  are rejected as `outside_study_region`. Python (`aegis/geo.py`) and Spark
  (`transforms.zone_id_col`) use the same arithmetic, which is checked by a test.
* **Distances:** Haversine, mean Earth radius 6371.0088 km, kilometres.
* **Station fan-out:** zones take rainfall from their nearest weather station
  within 12 km and water level from their nearest river gauge within 6 km
  (Spark cross join + Haversine + window rank). Zones outside the gauge radius
  have *no* water-level indicator, which lowers their coverage instead of
  pretending the river is low.
* **Nearby search:** `/api/incidents/nearby` and `spatial_analytics.nearby` filter
  by Haversine distance ≤ radius.
* No road network, official boundary or real incident location is used.

## Rule-based risk model

`aegis/risk.py` (one implementation used by Spark through a UDF, the API and tests):

```
risk_score = 100 · Σ_i∈available w_i · f_i  /  Σ_i∈available w_i        f_i = clamp((x_i − low_i) / (high_i − low_i), 0, 1)
coverage   = Σ_i∈available w_i / Σ_i w_i
```

| Indicator | Input (per zone and window) | Weight | low → high |
|---|---|---|---|
| rainfall | mean intensity, mm/hr | 0.30 | 2.5 → 50 |
| water_level | max level / danger level | 0.25 | 0.60 → 1.00 |
| incidents | weighted reports (verified 1, unverified 0.5, rejected 0) | 0.20 | 0 → 5 |
| traffic | share of disrupted observations | 0.15 | 0 → 0.75 |
| historical | zone's archived flood incidents / max over zones | 0.10 | 0 → 1 |

Categories (demonstration thresholds): Low < 25 ≤ Moderate < 50 ≤ High < 75 ≤ Critical.
Confidence from coverage: high ≥ 0.85, medium ≥ 0.60, low ≥ 0.40; below 0.40
the result is **"Insufficient data" with no score**. Missing indicators are
removed from numerator and denominator, so they never pull a score towards
"Low"; they reduce confidence instead. Report counts are treated as observed
(zero reports is a measured zero). Each assessment stores the contributions
(points per indicator, summing to the score), the missing indicators and a
plain-language explanation. All weights and thresholds are in `config/aegis.yaml`.

This index is an illustrative, explainable demonstration — not a validated forecast.

## HBase schema and access patterns

| Table | Row key | Families (versions, TTL) | Access pattern |
|---|---|---|---|
| `sensor_observations` | `source_id#event_ts_ms(13 digits)#event_id` | measurement(1, 30 d), location(1, 30 d), metadata(1, 30 d) | time-range scans per sensor (`row_prefix`, start/stop row); idempotent re-writes |
| `incident_records` | `incident_id` | incident, location, verification(3) | point lookup by id (`/api/incidents/{id}`) |
| `zone_risk_history` | `zone_id#live#window_start_ms` (streaming 5-min windows) and `zone_id#hourly#window_start_ms` (batch ETL hours) | risk(**5 versions**), summary, quality | current assessment per zone = reverse prefix scan of `zone_id#live#`, limit 1; history = reverse scan; every streaming update of a window adds a cell version. Separate series because a batch hour and a live window starting at the same instant would otherwise overwrite each other's cells (found during verification) |
| `processing_metrics` | `component#metric_ts_ms` | metrics(7 d), status(7 d) | latest metric per component (reverse scan) |
| `model_predictions` | `subject#(9999999999999 − ts_ms)#prediction_id` | input, output, meta | reverse-timestamp key: newest first with a forward scan |

Rows are sparse: absent values are simply not written (a zone without a
gauge has no `summary:level_ratio_max` cell). Fixed-width timestamps keep
lexical order equal to time order. The source-id prefix spreads writes across
regions; at production scale salting/pre-splitting would be added. Clients
talk to the Thrift server only (happybase), so no ZooKeeper/RegionServer
hostnames leak to the host.

## Parquet layout

Partitioning: silver by `event_date` (UTC), bronze by `topic`/`ingest_date`,
quarantine by `domain` or `quarantine_date`, live zone risk by `window_date`,
ML predictions by `predicted_date`. Batch ETL rebuilds silver/gold with
overwrite (`partitionOverwriteMode=static` for full rebuilds) and one file
per date partition. Timestamps are written as `TIMESTAMP_MICROS` (UTC).
Inspect with `scripts/run_batch_analytics.ps1 -Only sql` or any Parquet reader.

## Local demonstration vs production

Everything here runs on one machine: one Kafka broker (replication 1), HBase
in standalone mode on the local filesystem, Spark in local mode (`local[8]` batch, `local[4]` streaming). The code
uses distributed constructs (partitioned topics, Spark shuffles, per-partition
HBase writers, checkpoints) but **no distributed scalability has been measured**.
A production deployment would need a multi-broker Kafka cluster with
replication ≥ 3, HBase on HDFS with region pre-splitting, a Spark cluster
(YARN/Kubernetes) with checkpoints on HDFS/S3, authentication/TLS on every
service, and schema registry–managed event contracts.

### Resource requirements (measured)

The full stack runs in Docker Desktop's VM. On the verification machine (16
cores, Docker VM 7.6 GB RAM + 2 GB swap) the peak while the live stack and the
batch ETL ran together was ~6.3 GB of container memory (ETL ~3.0 GB, streaming
~2.0 GB, HBase ~1.0 GB, Kafka ~0.3 GB, API ~0.2 GB). Before the Spark resource
caps, memory pressure made the host pause the HBase JVM for ~42 s; the embedded
ZooKeeper session (then capped at 40 s) expired and HBase aborted. HBase now
uses a 6 s ZooKeeper tick / 120 s session timeout and restarts automatically.
Give Docker at least 8 GB; with less, stop the `streaming` and `producers`
services while running batch jobs (`scriptsun_e2e.ps1` pauses them itself).
