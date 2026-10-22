# AEGIS — end-to-end workflow

Two flows, traced through the actual code: **A** how the system starts, and
**B** how one weather reading travels from the simulator to a coloured zone on
the map. Diagram: [runtime_sequence.png](runtime_sequence.png) (source
[runtime_sequence.mmd](runtime_sequence.mmd)).

Working directory for every command: the project root (`D:\SEM 5\AEGIS`).
Prefix scripts with `powershell -ExecutionPolicy Bypass -File` if script
execution is restricted.

---

## Flow A — application start-up

The order matters only where a component needs another one already running:
topics must exist before producers publish; HBase tables before the streaming
HBase sink; the batch ETL's `gold/zone_hazard_history` before streaming can
add the historical indicator (otherwise that indicator is "not observed"). The
API and dashboard can start at any time — they show what exists and mark the
rest as unavailable.

| # | Step | Command | What happens in the code | Evidence of success | Common failures |
|---|---|---|---|---|---|
| 0 | Configuration | (automatic) | every process calls `aegis.config.get_config()`: reads `config/aegis.yaml`, applies `AEGIS__SECTION__KEY` overrides (compose sets `AEGIS__KAFKA__BOOTSTRAP_SERVERS=kafka:29092`, `AEGIS__HBASE__HOST=hbase`), validates weights and region | a `ConfigError` stops the process otherwise | a mistyped override creates a new key silently (check spelling) |
| 1 | Infrastructure | `scripts\start_infrastructure.ps1` | `docker compose up -d kafka hbase`; `Wait-Healthy` polls the health checks (Kafka ≤ 180 s, HBase ≤ 300 s) | `docker compose ps` shows both `(healthy)` | Docker Desktop not running; HBase slow first start; disk image full |
| 2 | Kafka topics | (inside step 1) `python -m producers.kafka_admin create` | `create_topics()`: 6 topics × 3 partitions, retention 7 d / 2 d; existing topics are left alone | JSON `{"weather-events": "created" \| "exists", …}` | broker not reachable |
| 3 | HBase tables | (inside step 1) `python hbase/create_tables.py` | `HBaseRepository.ensure_tables()` creates the 5 tables with their families, versions and TTLs | JSON listing the 5 tables; UI http://localhost:16010 | Thrift not up yet (retry) |
| 4 | Historical data | `scripts\generate_data.ps1` (once) | `producers.data_generator historical`: 60 days → `data/generated/historical/*.jsonl` + `manifest.json` + `data/reference/stations.json` | manifest with line counts and injected defects | — |
| 5 | Batch layer | `scripts\run_batch_analytics.ps1` | ETL (+ HBase load), RDD, SQL, spatial, risk ranking | `data/reports/dq_report_batch.json` with all checks `true`; exit code 0 | HBase down → ETL exits 1 with `hbase_load_failed=True` |
| 6 | Model | `scripts\train_model.ps1` | `model_training` + sample prediction | `data/reports/ml_evaluation.json`, `data/models/flood_severity_dt/latest.json` | training set < 200 rows or missing labels → `TrainingDataError` |
| 7 | Streaming | `scripts\start_streaming.ps1` | service `streaming`: `check_hbase()` (sink enabled/disabled), `load_static()` (station maps, hazard index), 3 queries start from their checkpoints | `data/state/streaming/status.json`: `"state": "running"`, `"hbase_sink": "enabled (hbase:9090)"`; Spark UI http://localhost:4040 | HBase unreachable at start → HBase sink disabled for that run (Parquet continues) |
| 8 | Producers | `scripts\start_producers.ps1` | service `producers`: `run_all --rate 4`; checks broker and that topics exist, then publishes | `docker compose logs producers`: `stats sent=… delivered=… failed=0` | topics missing → exit 2 |
| 9 | API | `scripts\start_api.ps1` | `lifespan`: builds services; starts the `LiveFeed` consumer thread and the model loader thread | http://localhost:8000/health → `{"status":"ok",…}`; `/docs` | port 8000 in use |
| 10 | Dashboard | `scripts\start_frontend.ps1` (2nd terminal) | Vite dev server; React fetches `/api/dataset`, `/api/zones`, `/api/overview` and opens `EventSource /api/stream` | http://127.0.0.1:5173, header shows "live updates connected" | API not reachable → red error box; CORS origin not allowed |
| 11 | Health | `GET /api/pipeline/status` or the Pipeline tab | Kafka offsets, HBase health, streaming query progress, producer status, lake counts | `"overall": "ok"`, `"issues": []` | issues list names the failing component |

Readiness is checked by Docker health checks (Kafka, HBase), by the producer
and streaming start-up checks, and by `/api/pipeline/status`. A full scripted
verification is `scripts\run_e2e.ps1` (14 steps; last run 14/14 PASS on
2026-10-22).

---

## Flow B — one weather reading, from simulator to screen

Concrete example from this repository: synthetic station **WXM-02**
(13.07651 N, 80.23178 E), which lies in zone **Z-R04-C02** and is the nearest
weather station (within 12 km) for zones Z-R04-C02, Z-R04-C03 and Z-R04-C04.
Zone Z-R04-C02's nearest river gauge is **WLS-07** (0.15 km).

| # | Stage | Code | What happens |
|---|---|---|---|
| 1 | **Origin** | `producers/data_generator.py → LiveGenerator.events()` | 50 sensors (12 weather, 8 river, 30 traffic) are scheduled in a heap; at 4 events/s with 85 % for sensors, each sensor emits about every 50 / (4 × 0.85) ≈ 14.7 s (± 20 %). The world state (`Environment`) is advanced on an 18× accelerated clock. |
| 2 | **Schema** | `Simulator.weather()` → `formats.weather_event()` | true rain at WXM-02 from the storm field, measured with ±8 % noise (values < 0.3 mm/hr → 0); temperature and humidity derived from time of day and rain. Result: a `wx-metric-1` dict `{event_id, source_type:"weather", source_id:"WXM-02", schema_version, synthetic:true, event_time, lat, lon, temperature_c, humidity_pct, rainfall_mm_hr, condition}`. `_restamp` sets `event_time` to the **real** current time in IST (`+05:30`). |
| 3 | **Defects (deliberate)** | `LiveGenerator._finalize()` | 3 % are re-stamped late (3–8 min = within the watermark, or 25–90 min = beyond it); a small share 2 days in the future; 6 % get a field defect from `AnomalyInjector`. Each is counted in `gen.counts`, never marked in the payload. |
| 4 | **Publish** | `base_producer.run()` → `AegisProducer.send()` | `basic_envelope_ok` (non-empty text ≤ 1 MB) → `produce("weather-events", key="WXM-02", value=json)`, `acks=all`, idempotent, lz4. Delivery callback counts `delivered`; a final failure goes to `data/dead_letter/producer_failures.jsonl`. |
| 5 | **Kafka** | topic `weather-events` | the key hash picks one of 3 partitions; all WXM-02 events stay in order in that partition; retained 7 days. |
| 6 | **Consumed by Spark** | `streaming_pipeline.Pipeline.kafka_source()` | every 10 s each of the 3 queries reads new offsets (≤ 20,000 per trigger) from its checkpoint; columns `value, topic, kafka_partition, kafka_offset, ingest_time (Kafka timestamp), origin="kafka"`. |
| 7 | **Validation + mapping** | `transforms.canonicalize(df, "weather", cfg)` | `from_json` with an all-string schema; `event_time` "…+05:30" cast to a UTC timestamp; `lat/lon` cast to double and range-checked; `rainfall_mm_hr` checked against 0–300; `"Heavy Rain"` → `heavy_rain`; `zone_id_col(13.07651, 80.23178)` → `Z-R04-C02`; `is_synthetic = true`, `data_source = aegis-simulator`, `observation_type = synthetic`; `late_arrival` flag if Kafka time − event time > 10 min; any failed check → `reasons` non-empty → `is_valid = false`. |
| 8 | **Persisted** | `bronze_dq_batch`, `silver_batch` | raw text → `lake/bronze/kafka_raw/topic=weather-events/ingest_date=…` (always); if invalid → `lake/quarantine/stream`; if valid: watermark 10 min + `dropDuplicatesWithinWatermark(event_id)` → `lake/silver_stream/weather/event_date=…` and HBase `sensor_observations`, row key `WXM-02#<event time in ms, 13 digits>#<event_id>`, families `measurement` (rainfall_mm_hr, temperature_c, …), `location` (lat, lon, zone_id), `metadata` (event_id, source_type, schema_version, origin, data_source …). Write count checked, 2 rows read back. |
| 9 | **Zone risk** | `zone_risk` query: `zone_signals()` → `signal_aggregations()` → `score()` | the station map turns the one reading into weather signals for Z-R04-C02, -C03, -C04; within the 5-minute event-time window the zone's mean rainfall is combined with WLS-07's max level ratio, the share of disrupted traffic observations, weighted reports and the zone's `historical_hazard_index` (from `gold/zone_hazard_history`); `aegis.risk.assess()` returns score, category, confidence, contributions, explanation (formula and example: [project_codebase_guide.md § 8](project_codebase_guide.md#8-risk-scoring)). Output mode *update*: the window is re-emitted each time new data changes it. |
| 10 | **Published for serving** | `zone_risk_batch()` | HBase `zone_risk_history` row `Z-R04-C02#live#<window_start_ms>` (the `risk` family keeps 5 versions of the score as the window updates); Parquet `lake/live/zone_risk/window_date=…`; Kafka `zone-risk-updates` (key `Z-R04-C02`, value = JSON assessment); `data/state/zone_risk_latest.json` (newest window per zone, used if HBase is down). |
| 11 | **API** | `LiveFeed._run()`; `ZoneService.current()` | the API's Kafka consumer (group `aegis-api-live-…`, latest offsets) receives the update, appends it to the SSE event log and raises a dashboard alert if the category is High or Critical. `GET /api/zones` reads the newest `Z-R04-C02#live#…` row (reverse prefix scan), compares it with the snapshot file, sets `data_origin = "live-stream"`, `stale = window ended > 900 s ago`, `read_from = "hbase"`. |
| 12 | **Dashboard** | `useLiveStream` → `App.onEvent` → `useApi(api.zones)` | the browser receives SSE `event: zone_risk`; after a 3-second debounce the zone list is reloaded; the Leaflet rectangle for Z-R04-C02 is recoloured by category, the Overview counts and the Zone risk tab (score, contribution bars, explanation, HBase history, cell versions) update; an alert appears on the Overview tab for High/Critical. |

**And later, in the batch layer:** the same raw record archived in
`lake/bronze/kafka_raw` is read by `etl_pipeline.load_stream_bronze()` the next
time the batch ETL runs, goes through the same `canonicalize()`, and ends up
in `silver/weather`, `gold/zone_hourly` and `gold/zone_risk_hourly` —
visible in the Historical analytics tab.

**What is not directly connected:** the ML model does not consume
`zone-risk-updates`; zone predictions are produced on demand by
`model_inference --zones` (reads `silver_stream`) or by the API's predict
endpoint. The rule-based score and the ML prediction are independent.

---

## Flow C — real and hybrid modes (batch only)

```
scripts\ingest_real_data.ps1   →  data_real\raw\historical\*.jsonl   (HTTP → validate → provenance)
scripts\run_real_data.ps1      →  ETL + real_data_layers → API :8001 → dashboard :5174 (-RealData)
scripts\run_hybrid_data.ps1    →  hybrid_generator + ETL + layers + model → API :8002 → dashboard :5175 (-Hybrid)
```

No Kafka or streaming is involved: the files are read by the same batch ETL
(`--input …/raw/historical --no-stream-bronze`) and served by a separate API
container with HBase and the live feed switched off.
