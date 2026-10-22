# Real-data test fixtures

Small excerpts of **real** responses and records downloaded on 2026-10-22 by
`python -m aegis.realdata.ingest` (period 2023-11-01 – 2023-12-30). They let
the tests run offline. Values are unmodified; files were only cut to a few
zones/days.

| File | Origin | Licence / terms |
|---|---|---|
| `open_meteo_era5_13.0N_80.25E_2023-12-03_04.json` | raw Open-Meteo Historical Weather API response, `models=era5`, one cell, 48 h | CC BY 4.0 — Weather data by Open-Meteo.com (contains modified Copernicus ERA5 data) |
| `era5_weather_2zones_2023-12-02_05.jsonl` | `wx-era5-1` records for zones Z-R03-C01, Z-R03-C02 (Cyclone Michaung) | as above |
| `rainfall_daily_2zones_2023-12-02_05.jsonl` | `rf-daily-1` records: IMD 0.25° gridded rainfall and NOAA GHCN-Daily IN020040900 for the same zones/days | IMD Pune (cite Pai et al. 2014); NOAA NCEI (cite Menne et al. 2012) |
| `ghcnd_IN020040900_2023-11-28_12-08.csv` | raw GHCN-Daily CSV rows, MADRAS MINAMBAKKAM | NOAA NCEI |
| `dfo_query_study_box.json` | raw ArcGIS feature-service response, Dartmouth Flood Observatory archive, events intersecting the study box | free for scholarly/education use with attribution (G.R. Brakenridge, DFO, Univ. of Colorado) |
| `gdacs_india_2023-11-01_12-30.json` | raw GDACS event-list response (TC, FL; India) | credit "Global Disaster Alert and Coordination System, GDACS" |
| `official_events_2023-11_12.jsonl` | all 26 `ev-official-1` records of the import: 24 DFO events (12 intersect the study box, 12 more within the 0.35° buffer, flagged `intersects_study_box: false`) and 2 GDACS cyclones | as above |
