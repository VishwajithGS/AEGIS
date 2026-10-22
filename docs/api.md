# AEGIS API (FastAPI)

Base URL `http://localhost:8000` · interactive OpenAPI/Swagger UI at
`http://localhost:8000/docs` · schema at `/openapi.json`.

The API never runs Spark jobs per request: Parquet is read with pyarrow
(15-second cache), HBase through the Thrift server with 4-second timeouts and
a 10-second circuit breaker, and the live feed comes from a background Kafka
consumer. The only Spark use is the MLlib model held in memory for
`POST /api/model/predict`. When a dependency is down the response says so
(`source_errors`, `issues`, `hbase_error`) instead of returning invented data.
Every zone/incident row carries `data_origin` (`live-stream` or
`batch-historical`); all data is synthetic.

| Method | Path | Purpose | Main sources |
|---|---|---|---|
| GET | `/health` | liveness + dependency reachability | HBase health, live-feed status, model state |
| GET | `/api/overview` | KPIs: processed counts, incidents, zones by category, DQ rates, processing status | Parquet counts, zone service, DQ |
| GET | `/api/zones?category=&sort=risk_score\|zone_id\|coverage&order=desc\|asc` | all 40 zones with geometry and the current rule-based assessment (`stale`, `data_origin`, `read_from`) | HBase `zone_risk_history` (`zone_id#live#…` series) → streaming snapshot → gold |
| GET | `/api/zones/{zone_id}` | zone detail: contributions, HBase history (48 windows), cell versions, 72 h batch history, station assignment, latest ML zone prediction | HBase, gold, reference registry |
| GET | `/api/incidents?category=&source_type=&verification=&zone_id=&origin=&q=&since_hours=&located_only=&limit=&offset=` | filtered, paginated incidents/reports (limit ≤ 500) | `silver/incidents` + `silver_stream/incidents` |
| GET | `/api/incidents/nearby?lat=&lon=&radius_km=&limit=&origin=` | hazards within `radius_km` (Haversine, km), nearest first | same |
| GET | `/api/incidents/{incident_id}` | one record (HBase `incident_records` first, then Parquet) | HBase / Parquet |
| GET | `/api/analytics/historical?zone_id=&days=` | daily incidents by category, rain/level, risk trend, zone-hours by category, hour-of-day, correlations, live trend; `real_data` (real mode only: IMD vs ERA5 daily rainfall, IMD category counts, gauge series, official events, validation summary; `available: false` otherwise) | gold tables, `live/zone_risk`, `gold/real_daily_rainfall`, `gold/station_daily_rainfall`, `gold/official_events` |
| GET | `/api/dataset` | data mode of the served data root (`synthetic`, `real-historical` or `hybrid`), period, sources with licences, `not_available` sources (real mode), `real_parts` / `synthetic_parts` (hybrid mode), label. `/api/overview` includes the same object as `dataset` plus `data_origin_note` | `<data_root>/dataset_manifest.json` (synthetic default when absent) |
| GET | `/api/analytics/data-quality` | batch DQ report + live micro-batch DQ + quarantine sample | `data/reports/dq_report_batch.json`, `live/dq_metrics`, quarantine |
| GET | `/api/pipeline/status` | Kafka topics/offsets, HBase health, streaming query progress, producers, lake counts, issues | Kafka admin, HBase, state files |
| GET | `/api/alerts` | alerts raised from `zone-risk-updates` + current High/Critical zones | live feed |
| GET | `/api/model/status` | state, version, provenance, held-out metrics, confusion matrix, importances, feature contract | model metadata |
| POST | `/api/model/predict` | predict severity for one feature record; persisted to HBase `model_predictions` | Spark MLlib PipelineModel |
| GET | `/api/model/predictions` | latest per-zone predictions (`predicted_severity` is null with `prediction_status = insufficient_data: …` for zones without weather observations) + recent API predictions | state file, HBase |
| GET | `/api/stream?since=` | Server-Sent Events: `zone_risk`, `data_quality`, `alert`, `status` | Kafka live feed |

## Validation and status codes

* Query parameters are validated (ranges, patterns); invalid values → **422**.
* Unknown zone / incident → **404**.
* `POST /api/model/predict`: body schema below (`extra` fields such as
  `severity` are rejected → 422); model not trained / Spark unavailable /
  still loading → **503** with the reason; otherwise 200.
* Unexpected errors → **500** `{"detail": "internal error: <Type>"}` (logged
  with the path; no stack traces or secrets in responses).

## Prediction request

```json
{
  "rain_3h_mm": 55.0, "rain_max_1h_mm_hr": 32.0, "level_ratio_max_3h": 0.92,
  "humidity_avg_3h": 94.0, "disrupted_ratio_1h": 0.45, "speed_ratio_avg_1h": 0.42,
  "reports_6h": 4, "water_obs_available": 1, "traffic_obs_available": 1,
  "zone_id": "Z-R04-C02"
}
```

`reports_6h`, `water_obs_available` and `traffic_obs_available` are required;
the measurements may be `null` (imputed with training medians and listed in
`imputed_features`). The response contains `predicted_severity`,
`probabilities` per class, `model_version`, `label_provenance` (synthetic),
`predicted_at`, `persisted` (HBase row key and read-back result) and a note
that the value is an ML estimate, not an observed severity or a warning.

## CORS and configuration

Allowed origins: `AEGIS_API_CORS_ORIGINS` (default `http://localhost:5173,http://127.0.0.1:5173`).
`AEGIS_API_LIVE_FEED=0` disables the Kafka consumer; `AEGIS_API_MODEL=0`
disables model hosting. Connection settings come from `config/aegis.yaml`
and `AEGIS__*` overrides (the compose file points the API at `kafka:29092`
and `hbase:9090`).

## Dashboard

The React dashboard (`frontend/`) uses these endpoints only through
`frontend/src/services/api.js`. Each tab has a URL fragment
(`http://127.0.0.1:5173/#map`, `#zones`, `#incidents`, `#analytics`, `#model`,
`#pipeline`). `scripts/ui_check.py` opens every tab in a real browser
(installed Edge or Chrome via Playwright), waits for project data, saves
screenshots to `data/reports/ui/` and fails on console errors or failed requests.
