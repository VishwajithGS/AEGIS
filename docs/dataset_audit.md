# AEGIS dataset audit

Audit date: 2026-10-22. Method: full search of the repository for data
generation (`random`, `rng`, `uniform`, `gauss`, `betavariate`, hard-coded
coordinates and records), inspection of every file under `data/`, and tracing
of each dataset to the code that consumes it. Classification rule: a dataset is
called **real** only if it was obtained from an identified external source;
realistic-looking values are not evidence.

## 1. Summary

**Every dataset currently used by AEGIS is synthetic.** All records are
produced by the simulator in `producers/` (`world.py`, `formats.py`,
`anomalies.py`, `data_generator.py`) from a fixed seed (42). No external data
is downloaded anywhere in the pipeline. Every raw record carries
`"synthetic": true`, the canonical tables carry `is_synthetic`, and the docs and
dashboard state that the data is synthetic. Nothing is mislabelled as real.

Only two non-data artefacts describe the real world, and both are approximate:

| Artefact | Where | Status |
|---|---|---|
| Study-region bounding box 12.85–13.25 °N, 80.10–80.35 °E ("Chennai metropolitan area, demo") | `config/aegis.yaml` → `region` | real coordinates, but the box itself is a demo choice, not an official boundary |
| Coastline used to keep sensors on land: `lon = 80.25 + (lat − 12.85) × 0.2` | `aegis/geo.py: approx_coast_lon` | a rough straight-line approximation, **not** real geography (the real coast is not a straight line) |

## 2. Inventory

| # | Dataset / source | Class | Content | Coverage | Schema / units / time / coordinates | Consumers | Used for | Essential? | If removed / replaced |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `data/generated/historical/weather.jsonl` (+ live `weather-events` topic) | **synthetic** | hourly-ish weather at 12 synthetic stations (9 metric `WXM-*`, 3 imperial `AWS-*`), 15-min steps | demo box; 60 days ending at the generator's end date; live = wall clock | `wx-metric-1`: `event_id, source_id, event_time` (ISO +05:30), `lat, lon` (WGS84), `temperature_c` °C, `humidity_pct` %, `rainfall_mm_hr` mm/h, `condition` text. `wx-imperial-2`: `obs_id, station, ts_ms` (epoch ms), `location.latitude/longitude`, `temp_f` °F, `rh_pct`, `precip_in_hr` in/h, `sky` METAR code | `spark_jobs/transforms.py` (weather mapping), `etl_pipeline.py`, `streaming_pipeline.py`, `risk_analytics.zone_signals`, `features.py`, `rdd_demo.py`, `sql_analytics.py` (q3), `hbase_sink` → `sensor_observations`, API `/api/analytics/historical`, dashboard | **risk** (rainfall indicator, weight 0.30), spatial (nearest station), historical analytics, **ML features** (`rain_3h_mm`, `rain_max_1h_mm_hr`, `humidity_avg_3h`) | yes (largest risk weight) | rainfall indicator becomes "not observed"; ML features imputed; RDD/SQL weather demos empty. Replaceable independently (schema-versioned topic) |
| 2 | `water_level.jsonl` (+ `water-level-events`) | **synthetic** | river level at 8 synthetic gauges on 3 **invented** rivers (`RIVER-A`, `RIVER-B`, `CANAL-C`, `producers/world.py: SYNTHETIC_RIVERS`) driven by synthetic rain | demo box, same period | `wl-1`: `water_level` in m/cm/ft per sensor, `unit`, `danger_level` (m, invented per gauge), `sensor_status`, ISO UTC | transforms (unit conversion, `level_ratio`), zone signals (gauges within 6 km), `features.py`, SQL q4, HBase, dashboard | **risk** (water-level indicator, weight 0.25), **ML** (`level_ratio_max_3h`, `water_obs_available`) | yes for coverage | water indicator not observed → zone coverage drops (most zones fall to low confidence). Replaceable independently |
| 3 | `traffic.jsonl` (+ `traffic-events`) | **synthetic** | speed, density and disruption at 30 synthetic road segments (random points, no road network) | demo box, same period | `tr-1`: `vehicle_density` veh/km, `avg_speed_kmph`, `free_flow_speed_kmph`, `disruption` text, local time without offset (Asia/Kolkata) | transforms, zone signals, `features.py`, SQL q5, correlations (gold), HBase, dashboard | **risk** (traffic indicator, weight 0.15), **ML** (`disrupted_ratio_1h`, `speed_ratio_avg_1h`, `traffic_obs_available`), traffic–hazard correlation | no (lowest risk weight) | traffic indicator not observed; correlation analytics empty |
| 4 | `incidents.jsonl` — citizen reports `pr-1` (+ `incident-reports`) | **synthetic** | reports about synthetic incidents, delayed and jittered, free-text categories, verification states, ~12 % without coordinates, plus false reports | demo box, same period | `report_id, category` text, `verification_status`, `confidence` 0–1, optional `lat/lon`, ISO +05:30 | transforms, zone signals (`weighted_reports`), `features.py` (`reports_6h`), nearby search, incident explorer, alerts, HBase `incident_records` | **risk** (incidents indicator, weight 0.20), **ML** feature `reports_6h` | yes for the "incidents" indicator | indicator not observed; incident explorer and nearby search empty |
| 5 | `incidents.jsonl` — archived incidents `hi-1` | **synthetic** | "historical incidents" with categories and **severity labels produced by `severity_rule`** (a formula over the simulator's hidden state) | demo box, same period | `incident_id, category, severity` (Low/Moderate/High/Critical), `affected_road`, `affected_area_km2`, ISO UTC | transforms, `zone_hazard_history` (gold), `historical_hazard_index`, **ML training targets** (`gold/ml_training_flood_severity`), SQL q1/q7/q9, dashboard | **risk** (historical indicator, weight 0.10), **ML labels** | essential for the current ML model | ML training set empty (model cannot be trained); historical indicator not observed |
| 6 | `data/generated/historical/manifest.json` | **synthetic** (metadata) | line counts, injected-defect counts, label counts | — | JSON | `etl_pipeline.dq_report` (injected-vs-detected check), `e2e_check.py` | data-quality verification | only for synthetic runs | the injected-vs-detected reconciliation has nothing to compare with |
| 7 | `data/reference/stations.json` | **synthetic** | registry of the synthetic stations, gauges and segments (positions, units, danger levels) | demo box | JSON | `spatial_analytics.nearest_station_map`, API zone detail (station assignment) | spatial (station → zone fan-out) | yes for weather/water fan-out | zones lose station assignment |
| 8 | `data/reference/zones.json`, zone grid in code | **derived** (not data) | 40-cell 0.05° analysis grid over the box | demo box | `Z-R<row>-C<col>`, bounds, centres | everything spatial | zoning | yes | — (a grid, not a dataset; stays) |
| 9 | `data/sample/*` (committed) | **synthetic** | 2-day version of 1–7 with fixed end date 2026-10-15 | demo box | as above | quick demos, docs | demos | no | nothing breaks |
| 10 | Live Kafka streams (`producers/run_all.py`) | **synthetic** | same generator, accelerated simulation clock (one 6-hour storm in ~20 min), wall-clock timestamps | demo box, real time | as 1–4 | Structured Streaming, live dashboard, alerts | live risk, alerts, live ML inference | for the live demo | live mode stops |
| 11 | Injected defects (`producers/anomalies.py`) | **synthetic test data** | malformed JSON, missing fields, out-of-range, wrong types, bad coordinates, unknown schema, duplicates, late/future timestamps | — | — | validation tests, DQ reports | data-quality testing | for testing | keep for tests regardless of migration |
| 12 | ML training set `gold/ml_training_flood_severity` | **derived from synthetic** | point-in-time features from 1–4 + labels from 5 | — | `docs/ml_model.md` | `model_training.py` | ML | for ML | — |
| 13 | ML metrics `data/reports/ml_evaluation.json` | **derived from synthetic** | accuracy 0.677 etc. | — | JSON | docs, dashboard | reporting | — | must be recomputed after any migration; they say nothing about real-world skill |
| 14 | Rule-based risk scores (gold/live) | **derived** | weighted indicators | — | `aegis/risk.py` | API, dashboard, HBase | risk | — | change automatically with their inputs |
| 15 | API test fixtures (`tests/unit/test_api.py`) | **hand-written test data** | 3 zone-hours, 3 incidents | — | — | tests only | tests | — | — |

Hard-coded synthetic definitions inside code (not files): `SYNTHETIC_RIVERS`
(invented river polylines), `historical_storms` / `live_storms` (invented
storm schedules), incident rates `FLOOD_RATE` / `OTHER_RATES`, `severity_rule`
and its thresholds, report text templates (`formats.DESCRIPTIONS`), and the
coastline approximation.

## 3. Pipeline paths that depend on synthetic-only properties

* **Injected-vs-detected reconciliation** (`etl_pipeline.dq_report`) needs the
  generator manifest; real data has no injected defects to compare against.
* **ML training** needs `hi-1` severity labels; real sources do not provide them
  at incident/zone level (see the migration plan).
* **Report counts as observed zeros**: `risk_analytics.signal_aggregations`
  treats "no report in the window" as an observed zero. That is valid only when
  a report source exists. Without one, the incidents indicator must be "not
  observed".
* **Historical hazard index** is computed from archived synthetic incidents;
  with no incident archive it currently becomes 0 for every zone, which would
  wrongly look like "observed low hazard".
* **Live time compression** (`model.live_bucket_seconds = 200`) exists only
  because synthetic storms are accelerated; real data runs at real time.

## 4. Conclusions

1. All event data (weather, water level, traffic, reports, archived incidents,
   severity labels) is synthetic and correctly labelled as such.
2. The datasets are independent at the pipeline level (separate topics, schema
   versions, silver tables), so they can be replaced one at a time.
3. The ML labels are the hardest dependency: they come from a formula over the
   simulator's hidden state and have no real counterpart at the same granularity.
4. Replacement candidates, their verification evidence and the resulting plan
   are in [dataset_migration_plan.md](dataset_migration_plan.md).

## 5. Status after the migration (2026-10-22)

The synthetic datasets above are **unchanged and still the default**. A
separate real data root (`data_real/`) now provides real counterparts where a
verified source exists:

| Synthetic dataset | Real counterpart in `data_real/` | Status |
|---|---|---|
| weather (`wx-metric-1`, `wx-imperial-2`) | ERA5 hourly reanalysis (`wx-era5-1`) | replaced with conversion — reanalysis, not gauge observations |
| — (no synthetic equivalent) | IMD gridded + NOAA gauge daily rainfall (`rf-daily-1`) | added, used to validate ERA5 |
| archived incidents (`hi-1`) | DFO / GDACS regional events (`ev-official-1`) | complementary only — stored as events, never as incidents |
| water level (`wl-1`) | — | not replaced (no accessible gauge source) |
| traffic (`tr-1`) | — | not replaced (no open source) |
| citizen reports (`pr-1`) | — | not replaced (no open source) |
| ML severity labels | — | not replaced (no real label exists) |
| injected-defect manifest | — | not applicable to real data (check reported as n/a) |

The three synthetic-only pipeline paths in §3 were made mode-aware: the
injected-defect and ML checks report *not applicable* in real mode,
`risk.reports_observed = false` makes the incidents indicator "not observed",
and the hazard index is NULL (not 0) without an incident archive.

A third, **hybrid** data root (`data_hybrid/`, `scripts\run_hybrid_data.ps1`)
fills the "not replaced" rows above with synthetic data generated from the
real ERA5 rainfall, so every zone gets a risk score and the ML model can be
trained on the real cyclone period. Those rows stay synthetic and are tagged
`data_source = aegis-simulator-era5-driven` (see
[dataset_migration_plan.md](dataset_migration_plan.md) §5b).
