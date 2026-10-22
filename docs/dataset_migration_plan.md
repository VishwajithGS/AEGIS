# AEGIS synthetic-to-real dataset migration plan

Companion to [dataset_audit.md](dataset_audit.md). Every "verified" statement
below is backed by a request made from the project machine on 2026-10-22
(command and result in §1). Where a source could not be accessed, the blocker
is stated; nothing is assumed from documentation alone.

## 1. Candidate real sources and evidence

| Source | Official link | Evidence (this machine, 2026-10-22) | Coverage | Fields actually obtained | Access / auth | Licence / terms | Volume | Limitations |
|---|---|---|---|---|---|---|---|---|
| **IMD 0.25° gridded daily rainfall** (official, India) | https://www.imdpune.gov.in/cmpg/Griddata/Rainfall_25_NetCDF.html | `POST RF25.php RF25=2023` → HTTP 200, 25.4 MB classic NetCDF-3; parsed with `aegis/realdata/netcdf3.py`; cell 13.0 °N 80.0 °E = **244.0 mm on 2023-12-04**, 97.3 mm on 2023-11-30 | India land, 6.5–38.5 °N, 66.5–100 °E; years 1901–2025 offered | `RAINFALL` (mm/day, −999 missing), `LATITUDE`, `LONGITUDE`, `TIME` (days since 1900-12-31) | web form (POST), **no login** | no licence text on the page; page asks to cite Pai et al. (2014) | 25 MB / year | **daily** totals read at 08:30 IST — whether date D is the 24 h starting or ending then is **not confirmed** (ERA5 test inconclusive); 0.25° cells (~27 km); coastal Chennai cell (13.0, 80.25) is masked as sea; gridded from IMD gauges |
| **NOAA GHCN-Daily**, station `IN020040900` MADRAS/MINAMBAKKAM (WMO 43279) | https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily | `GET …/access/IN020040900.csv` → 200, 4.7 MB; **PRCP 2023-12-04 = 2499 (= 249.9 mm)**, 12-05 = 188.0 mm | 1901-01-01 – 2025-08-24 | `PRCP` (tenths of mm), `TMAX`, `TMIN`, `TAVG` (tenths °C), quality/source attributes (`S` = Global Summary of the Day) | HTTPS, no auth | NOAA page states no use constraints; cite Menne et al. (2012) | 4.7 MB (full history) | one gauge in the box; daily only; Nungambakkam `IN020070300` only 1951–1970 |
| **NOAA ISD-Lite** station 432790-99999 | https://noaa-isd-pds.s3.amazonaws.com/isd-lite/data/2023/432790-99999-2023.gz | 200, 68 KB; Nov 25 – Dec 8 2023: hourly air temperature 336/336 hours, **1-h precipitation 0/336, 6-h 9/336** | hourly, multi-year | temperature, dew point, pressure, wind, (precip almost always missing here) | HTTPS (AWS open data), no auth | NOAA open data | ~70 KB / year | **no usable hourly rain observations** for Chennai |
| **Meteostat** (aggregates ISD + model) | https://dev.meteostat.net/data/timeseries/hourly | `data.meteostat.net/hourly/2023/43279.csv.gz` → 200; per-value source flags: `temp_source = isd_lite`, `rhum_source = isd_lite`, **`prcp_source = dwd_mosmix` for 8760/8760 hours** | hourly | temp, rhum, prcp (+ `_source` columns) | no key | licence page not verified (WebFetch could not read it) | 85 KB / year | precipitation is **model output (DWD MOSMIX), not observed**; adds nothing over ISD for observations |
| **ERA5 reanalysis** via **Open-Meteo Historical Weather API** | https://open-meteo.com/en/docs/historical-weather-api | `archive-api.open-meteo.com/v1/archive?…&models=era5` → 200; 13.0 °N 80.25 °E, 3–4 Dec 2023: hourly precipitation sum **272.9 mm**, max 28 mm/h; temperature, RH present | 1940 – ~5 days ago, global, 0.25° | `precipitation` (mm per hour), `temperature_2m` (°C), `relative_humidity_2m` (%), cell lat/lon/elevation | HTTPS, no key | data CC BY 4.0, credit "Weather data by Open-Meteo.com"; free API: non-commercial, < 10 000 calls/day | ~1–2 MB per 60 days × 40 points | **reanalysis (model + assimilated observations), not station measurements**; 0.25° cell averages smooth convective extremes. `models=era5_land` returned **0 mm** for the cyclone at Chennai → rejected for rainfall |
| **NASA POWER** hourly | https://power.larc.nasa.gov/ | `api/temporal/hourly/point?parameters=PRECTOTCORR,…` → 200; same 48 h: 229.2 mm | 2001 – near present | `PRECTOTCORR` (mm/h), `T2M`, `RH2M`; header sources `MERRA2`, `POWER` | no key | NASA open data | small | MERRA-2 model-based; used only as a cross-check |
| **GloFAS river discharge** via Open-Meteo Flood API | https://open-meteo.com/en/docs/flood-api | `flood-api.open-meteo.com/v1/flood?…river_discharge` → 200; coastal cell null; inland Adyar/Cooum cells rise from ~25 to **625.6 m³/s on 2023-12-05** | 1984 – present (+ forecasts), 5 km | `river_discharge` (m³/s, daily) | no key | Copernicus EMS data via Open-Meteo (CC BY 4.0) | small | **modelled discharge, not a gauge water level**; no danger levels; daily |
| **GDACS** event API (UN OCHA / EC JRC) | https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH | `eventlist=TC;FL&country=India&fromDate=2023-11-01…` → 200; **"Tropical Cyclone MICHAUNG-23", 2023-12-03 – 12-04, alert Orange** | global, ~2000 – present | `eventtype, eventid, name, fromdate, todate, alertlevel, alertscore, severitydata, country, glide`, point geometry | no key | per JRC report reuse under CC BY 4.0 with credit "GDACS"; automatic alerts, not official national warnings | KB | event level (cyclone track / country), not street incidents |
| **Dartmouth Flood Observatory** Global Active Archive | https://floodobservatory.colorado.edu/Archives/index.html (ArcGIS feature service `Global_Archive_of_large_flood_events`) | feature-service query for the Chennai box → 200; **12 events intersect the box**; a second query with a 0.35° buffer adds 12 more (**24 events 1989–2015**), incl. **ID 4309 (2015 Chennai floods), 2015-11-10 – 12-04, severity 2, 180 dead, 400 000 displaced**, whose polygon does *not* intersect the exact box — buffered events are flagged `intersects_study_box = false` | global 1985 – (service ends 2015 here) | `ID, BEGAN, ENDED, SEVERITY` (1, 1.5, 2), `DEAD, DISPLACED, MAINCAUSE, AREA` (km²), `GLIDENUMBE`, event polygon | no key | free for scholarly/education use with attribution (G.R. Brakenridge, DFO, Univ. of Colorado) | KB | **regional polygons (40 000 – 2 000 000 km²)**: same value for every zone; not incidents; no 2023 events in this service |
| **NDMA SACHET** CAP alert feed (IMD, CWC, SDMAs) | https://sachet.ndma.gov.in/cap_public_website/rss/rss_india.xml | 200, 82 KB, 100 latest items; CAP 1.2 XML per item; sample: IMD Chennai **"Moderate Rain", severity Moderate, urgency Expected, certainty Likely**, effective/expires, areaDesc (district) | India, **live** (rolling window of ~3 h) | `identifier, sender, sent, msgType, event, urgency, severity, certainty, effective, expires, headline, areaDesc`, sometimes `polygon` | no key | no reuse terms found; official public warnings | KB per poll | warnings, not observed incidents; district granularity; no archive (must be polled) |
| **Global Flood Database** (MODIS) | https://developers.google.com/earth-engine/datasets/catalog/GLOBAL_FLOOD_DB_MODIS_EVENTS_V1 | catalogue page read: 913 events 2000-02-17 – 2018-12-10, bands `flooded, duration, clear_views, clear_perc, jrc_perm_water`, `dfo_severity` | global | flood extent / duration rasters | **BLOCKED: Google Earth Engine account required** | CC BY-NC 4.0 | per event | ends 2018; extent, not severity labels |
| **NASA GPM IMERG** half-hourly | https://gpm.nasa.gov/data/imerg | GES DISC directory listing 200; file download → **302 to Earthdata login** | 2000 – present, 0.1°, 30 min | precipitation | **BLOCKED: NASA Earthdata account (free) required** | NASA open data | ~2–5 MB per half-hour global file | needs subsetting tools |
| **ReliefWeb API** | https://apidoc.reliefweb.int/ | `api.reliefweb.int/v2/disasters?appname=…` → **403 "not using an approved appname"** | — | — | **BLOCKED: approved appname must be requested** | — | — | text reports |
| **data.gov.in API** | https://api.data.gov.in/ | → **400 without API key** | — | — | **BLOCKED: free API key** | Govt. Open Data Licence – India | — | dataset-specific |
| **India-WRIS** (water resources) | https://indiawris.gov.in/wris/ | **TLS connection failure** (curl rc 35) from this machine | — | — | **UNREACHABLE** here | — | — | — |
| **CWC flood forecasting** | https://ffs.india-water.gov.in/ | site reachable (200); no documented open data API found | — | — | web portal | not stated | — | scraping a portal without terms is not appropriate |
| **IMD mausam portal** | https://mausam.imd.gov.in/ | **timeout** from this machine | — | — | — | — | — | — |
| **Real-time traffic for Chennai** | — | no open source found (TomTom/HERE are commercial with keys; TomTom's public "traffic index" is a yearly summary) | — | — | — | provider ToS typically forbid storing data | — | **not replaceable** with verified open data |
| **Citizen incident reports** | — | no open, structured, licensed source found | — | — | — | — | — | **not replaceable** |

## 2. Mapping table

Legend for **Feasibility**: **D** direct replacement · **C** replacement requiring schema/unit conversion · **P** partial replacement · **X** complementary source (cannot replace the original field) · **N** not currently replaceable (no verified source).

| Existing synthetic input | Proposed real source | Field mapping | Required transformation | AEGIS components affected | Feasibility |
|---|---|---|---|---|---|
| weather `rainfall_mm_hr` (hourly) | ERA5 via Open-Meteo, one record per zone centre per hour | `precipitation` (mm in the hour) → `rainfall_mm_hr` | hourly accumulation → intensity (same number); UTC already; cell → zone by querying each zone centre; new schema version `wx-era5-1` with provenance | transforms, ETL, zone signals, risk (rainfall indicator), features, SQL q3, RDD weather demo | **C** (reanalysis, not observation; ~27 km cells) |
| weather `temperature_c`, `humidity_pct` | ERA5 (same records) | `temperature_2m` → `temperature_c`, `relative_humidity_2m` → `humidity_pct` | none | as above | **C** |
| weather `condition` | — | none | left NULL → `condition = unknown` (not invented) | display only | **N** |
| daily rainfall (new; validates the above) | IMD gridded 0.25° + GHCN-D Minambakkam | `RAINFALL` mm/day, `PRCP`/10 mm/day | NetCDF-3 parsing; nearest valid land cell per zone; 24 h window at 03:00 UTC (08:30 IST) — start/end attribution assumed, both alignments reported; tenths → mm | new gold table `real_daily_rainfall` + ERA5-vs-IMD comparison | **X** (official observed, but daily; cannot drive hourly windows) |
| water level `water_level`, `danger_level`, `level_ratio` | GloFAS discharge (Open-Meteo) | `river_discharge` m³/s | would need a defensible threshold (e.g. long-term return period per cell) to form a ratio — **a different quantity**, labelled as a proxy | zone signals (water indicator), features | **P / X** — not implemented yet; needs your decision (§6) |
| water level from real gauges | CWC / India-WRIS | — | — | — | **N** (no accessible open API from here) |
| traffic speed / density / disruption | — | — | — | traffic indicator, correlations, ML traffic features | **N** |
| citizen reports `pr-1` | — | — | — | incidents indicator, explorer, nearby search, alerts | **N** |
| live official warnings (new) | NDMA SACHET CAP | `event, severity, urgency, certainty, effective, expires, areaDesc` | poll RSS, fetch CAP, filter areaDesc for Chennai/Tiruvallur/Chengalpattu, map severity Minor/Moderate/Severe/Extreme | could feed a new "official warning" indicator / dashboard panel | **X** — not implemented yet (§6) |
| archived incidents `hi-1` (zone level) | DFO archive (1989–2015), GDACS events | `BEGAN/ENDED, SEVERITY, DEAD, DISPLACED, MAINCAUSE` / `alertlevel` | events are regional, not point incidents → stored as a separate `official_events` table, never as incidents | historical analytics (event list); **not** the zone hazard index | **X** |
| `historical_hazard_index` (zone level) | Global Flood Database (flood frequency per pixel) | `flooded` per event → per-zone flood frequency | Earth Engine export | risk historical indicator | **N here** (Earth Engine account needed) |
| ML severity labels | none at incident/zone level (DFO severity is regional, 24 events) | — | — | ML | **N** — see §4 |
| injected defects / manifest | — | — | keep for synthetic test runs only | tests, DQ | keep (test data) |

## 3. Data categories (kept distinct everywhere)

| Category | Examples after migration | How it is labelled |
|---|---|---|
| Historical observed measurements | IMD gridded daily rainfall (gauge-based), GHCN-D daily rainfall, ISD hourly temperature | `observation_type = observation` (gridded: `observation_gridded`) |
| Reanalysis / model output | ERA5 hourly rain/temp/RH, NASA POWER, GloFAS discharge, Meteostat precipitation | `observation_type = reanalysis` / `model` |
| Officially reported events and warnings | DFO events, GDACS alerts, SACHET CAP warnings | `official_events` table, `record_type = event` / `warning` |
| Derived features and aggregates | zone-hour rainfall, daily totals, ERA5-vs-IMD bias | gold tables, computed |
| Rule-based risk scores | `aegis/risk.py` output | `kind = rule-based index`, with coverage/confidence |
| ML predictions | DecisionTree output | `kind = ML estimate`, label provenance shown |
| Synthetic development/test data | everything from `producers/` | `is_synthetic = true`, `data_source = aegis-simulator` |

## 4. ML audit

| Question | Finding |
|---|---|
| Which dataset trains the model? | `gold/ml_training_flood_severity`, built by the batch ETL from archived synthetic flood incidents (`hi-1`) and point-in-time features from synthetic weather, water, traffic and reports |
| Are labels synthetic? | **Yes.** `producers/data_generator.py: severity_rule` computes them from the simulator's hidden state (true rain, hazard, recent incidents, disruption) plus noise, cut at fixed thresholds |
| Do the real sources provide this label? | **No.** No verified source gives Low/Moderate/High/Critical severity per incident or per zone-hour. DFO `SEVERITY` (1/1.5/2) is per regional event (12 events since 1989 intersecting the box, 24 within 0.35° of it; each polygon covers every zone at once); Global Flood Database `dfo_severity` is the same DFO class (and needs Earth Engine); GDACS alert levels are per event |
| Can the model be trained on real data without changing its objective? | **No.** The objective (incident severity per zone) has no real target |
| Must features / classes / target change? | Yes, for any real-data model. A defensible option supported by the verified data: predict the **IMD rainfall category of the next 24 h** for a zone (IMD's published daily intensity classes: no rain < 2.5 mm, light 2.5–15.5, moderate 15.6–64.4, heavy 64.5–115.5, very heavy 115.6–204.4, extremely heavy ≥ 204.5 mm), using only past ERA5/IMD values as features. This is a **derived target from observed measurements with official thresholds** — not a flood-severity label, and must be called that |
| Do the existing model and metrics remain valid? | They remain valid **only as a synthetic-label demonstration** (accuracy 0.677 on the simulator). They say nothing about real-world skill and do not transfer to real inputs |
| Recommendation | Keep the current model, clearly labelled synthetic. Add a separately named real-data model only after you approve the new objective (§6). Do not relabel the existing model's output as real |

## 5. Migration strategy (implemented in this round)

Principles: additive, reversible, no change to the synthetic workflow, no
mixing. Real data lives in its own data root (`data_real/`), with its own
lake, reports and manifest. Synthetic data stays in `data/`.

1. **Ingestion layer** `aegis/realdata/` with one adapter per verified source:
   `era5` (Open-Meteo), `imd` (gridded NetCDF), `ghcnd` (NOAA), `dfo`
   (ArcGIS service), `gdacs` (event API). Each adapter validates the response
   schema, normalises units, time (UTC) and coordinates, attaches provenance
   (`data_source`, `source_url`, `licence`, `retrieved_at`, `observation_type`,
   `is_synthetic=false`) and writes JSON-lines plus a manifest with a SHA-256
   per raw response. Event ids are deterministic (`source + location + time`),
   so re-running an import produces identical files and the ETL de-duplicates
   on `event_id`. A failed download is an error; nothing synthetic is substituted.
2. **Pipeline compatibility**: ERA5 records use the existing `weather-events`
   schema family with a new version `wx-era5-1`, mapped by
   `spark_jobs/transforms.py` into the same canonical weather columns, so
   ETL, risk, features, SQL and RDD jobs run unchanged. Canonical tables gain
   `data_source` and `observation_type`.
3. **Honest risk without missing sources**: a config switch
   `risk.reports_observed` (true for synthetic data, false for real data) makes
   "no report" count as *not observed* instead of zero, and the historical
   hazard index becomes *not observed* when there is no incident archive
   (previously 0). With only rainfall observed (30 % of the indicator weight)
   the rule-based index correctly reports **Insufficient data**; the
   rainfall itself is still shown and analysed.
4. **Separate validation outputs**: `gold/real_daily_rainfall` (IMD and
   GHCN-D per zone/day with IMD intensity category) and an ERA5-vs-IMD
   comparison report; `gold/official_events` (DFO + GDACS).
5. **Mode labelling**: `data_real/dataset_manifest.json` declares
   `data_mode = real-historical`; the API returns it and the dashboard banner
   shows it instead of the synthetic notice. Real-mode API runs with HBase and
   the Kafka live feed disabled, so synthetic live rows cannot appear.

6. **Operations**: `scripts\ingest_real_data.ps1` (download + normalise),
   `scripts\run_real_data.ps1` (batch ETL in real mode + real-data layers +
   real-mode API container `api-real` on port 8001, compose profile `real`),
   `scripts\start_frontend.ps1 -RealData` (dashboard on port 5174). The
   synthetic stack (`api` on 8000, dashboard on 5173) is untouched and can
   run at the same time.

## 5a. Results of the implemented migration (run 2026-10-22, period 2023-11-01 – 2023-12-30)

All figures below come from `data_real/raw/historical/manifest.json`,
`data_real/reports/dq_report_batch.json` and
`data_real/reports/real_data_validation.json` produced by the commands in §5, item 6.
They describe **agreement between data sources**, not model skill.

| Item | Result |
|---|---|
| ERA5 hourly records | 57 600 = 40 zones × 1 440 hours, 0 missing values. The 40 zone centres fall in only **2 ERA5 cells** (13.0 °N 80.25 °E: 25 zones; 13.25 °N 80.25 °E: 15 zones), so neighbouring zones carry identical rainfall |
| IMD gridded daily records | 2 400 = 40 zones × 60 days, 0 missing; **3 IMD land cells** used; nearest-cell distance up to 37.9 km (coastal cells masked as sea) |
| NOAA GHCN-D gauge records | 60 days, **25 days without a report** (kept as missing, not filled) |
| Official events | 26: 24 DFO (12 intersecting the box, 12 within the 0.35° buffer) + 2 GDACS (MICHAUNG-23 Orange, MIDHILI-23 Orange — the latter 1 617 km away, from the country-level search) |
| Re-run idempotency | second import over the cached responses produced byte-identical JSON-lines files |
| Batch ETL (real mode) | 57 600 records, 57 600 valid, 0 rejected, 0 duplicates; all reconciliation checks PASS, the injected-defect and ML-training-set checks are *not applicable* (no injected defects, no labels) |
| Rule-based risk | 57 600 of 57 600 zone-hours **Insufficient data** (only rainfall is observed = 30 % of the indicator weight, minimum is 40 %). This is the intended, honest result |
| ERA5 vs IMD, region daily mean (60 days) | r = 0.784, mean difference −3.47 mm/day, MAE 10.8 mm; period totals IMD 939.6 mm vs ERA5 731.3 mm (ERA5 22 % lower) |
| ERA5 vs IMD, zone-days (2 400) | r = 0.745 (assumed window) / 0.733 (alternative window); IMD category agreement 58.4 % |
| Heavy rain (IMD ≥ 64.5 mm) | 195 zone-days; ERA5 also heavy on 59; ERA5 heavy but IMD not on 21 |
| Largest disagreement | 2023-12-05: IMD region mean 121.8 mm, ERA5 0.7 mm (consistent with a day-attribution offset or ERA5 timing error; not resolved) |
| Gauge vs IMD cell (35 days) | r = 0.99, MAE 3.1 mm — IMD's grid is built from IMD gauges, so this is expected and not independent |
| Gauge vs ERA5 (35 days) | r = 0.706 (assumed window) / 0.793 (alternative window); ERA5 total 35 % below the gauge |
| Dashboard (real mode) | 7 of 7 tabs PASS in `scripts/ui_check.py --real`, 0 console errors, 0 failed requests |

What this means for AEGIS: ERA5 reproduces the timing of the cyclone
event but underestimates the peak and the totals, and it is a ~27 km cell
average. It is the only verified *hourly* rainfall source available without
an account; IMD remains the reference for daily amounts.

## 5b. Hybrid mode: synthetic fill for the missing sources (added 2026-10-22 on request)

The project owner asked for the gaps of the real mode to be filled with
synthetic data instead of showing "Insufficient data". This is done as a
separate, explicitly labelled data root, so the pure real mode (§5a) stays
available and nothing is mixed silently.

| Aspect | Implementation |
|---|---|
| Real parts | ERA5 hourly weather, IMD/NOAA daily rainfall, DFO/GDACS events — copied unchanged from `data_real/` |
| Synthetic parts | river gauges, road segments, citizen reports, archived incidents with severity labels — from the existing simulator (`producers/data_generator.py`, unchanged) |
| Coupling | `producers/hybrid_generator.py` replaces the simulator's random storm field (`Environment.rain`) with the real ERA5 rainfall of the zone and hour, so all synthetic signals respond to the real rain |
| Labelling | synthetic records: `synthetic: true`, `data_source = aegis-simulator-era5-driven`, `observation_type = synthetic`; `dataset_manifest.json` `data_mode = hybrid` with `real_parts` / `synthetic_parts`; amber "Hybrid data" banner on the dashboard; ML tab and incident explorer say the incidents and labels are synthetic |
| Model setting | wetness time constant 24 h instead of the simulator's 6 h. ERA5 is a ~27 km cell average (Michaung peak ~28 mm/h vs 35–95 mm/h synthetic cells); with 6 h the simulated rivers peak at 0.77 × danger level although IMD recorded "extremely heavy" rain (≥ 204.5 mm/day) on 22 zone-days; 24 h gives 1.39 ×. This is a synthetic-model choice, not a measured catchment property (`--wetness-tau-h` to change it) |
| Defects | none injected; the injected-defect check is "not applicable" |
| Commands | `scripts\run_hybrid_data.ps1`, `scripts\start_frontend.ps1 -Hybrid` (API :8002, dashboard :5175) |

Results of the run on 2026-10-22 (seed 42, 15-minute steps):

| Item | Result |
|---|---|
| Records | 282,408 = 57,600 real ERA5 + 46,080 synthetic water level + 172,800 synthetic traffic + 5,928 synthetic incidents/reports (1,894 archived incidents, 4,034 reports) |
| Batch ETL | 0 rejected; all applicable reconciliation checks PASS; ML training set 1,549 flood-related incidents with all 4 labels (Low 490, Moderate 482, High 377, Critical 200) |
| Rule-based risk | 57,600 zone-hours scored, **0 "Insufficient data"**: Low 56,639 · Moderate 733 · High 228; peak 73.5 on 2023-12-04 (simulated river peak 1.39 × danger level), 61.0 on 12-05 |
| Simulated incidents | flood-related incidents concentrated on 2023-12-04 (767) and 12-05 (372) |
| MLlib (SYNTHETIC labels) | accuracy 0.842, weighted F1 0.845, majority baseline 0.274. **Caveat:** the time-based split (cutoff 2023-12-05 06:47 UTC) leaves a test set of Low 224, Moderate 85, High 1, Critical 0, so the score mostly measures easy low-severity cases; High/Critical skill is not measured. Not comparable with the synthetic-mode model and not real-world skill |
| Zone predictions | 40 zones scored as of 2023-12-31 (Low 39, Moderate 1) |
| Dashboard | `scripts/ui_check.py --hybrid`: 7/7 tabs PASS, 0 console errors |

## 6. Not implemented — needs your decision

| Item | Why it needs a decision |
|---|---|
| GloFAS discharge as a proxy water indicator | changes the meaning of the water indicator (discharge vs danger level); threshold choice (e.g. 2-year return period) must be agreed |
| SACHET live warnings | real live data, but a new indicator/panel and a polling producer; terms of reuse not found |
| Real-data ML objective (IMD next-day rainfall category) | changes the model's target and the meaning of the ML tab |
| Kafka historical replay of real data | replayed events carry past timestamps; dashboard must show "historical replay" mode |
| Accounts: NASA Earthdata (IMERG), Google Earth Engine (Global Flood Database), ReliefWeb appname, data.gov.in key | only you can register; each would unlock a listed gap |
| Real traffic and citizen reports | no verified open source; commercial APIs would need keys and their terms forbid storage |
