# Supporting ML component — flood-incident severity classifier (Spark MLlib)

AEGIS is primarily a Big Data engineering pipeline. This component satisfies
the course requirement for a Spark ML model with one small, genuine
classification experiment built on data produced by the pipeline itself.

> **Labels are SYNTHETIC.** They are produced by the simulator's documented
> rule (below), not by real emergency records. Metrics show how well a
> decision tree recovers that rule from noisy, partial sensor observations.
> They are **not** evidence of real-world flood-forecasting accuracy, and
> predictions are **not** observed severities or official warnings.

## Objective and target

Predict the severity class of a flood-related incident (category `flood` or
`waterlogging`) from conditions observable *before* the incident.
Target `label` ∈ {Low, Moderate, High, Critical} (demonstration classes, not
an official scale), indexed with a fixed mapping Low=0, Moderate=1, High=2, Critical=3.

## How the synthetic labels are created

`producers/data_generator.py: severity_rule` uses the simulator's TRUE latent state:

```
latent = 0.40·clamp(rain_3h_true / 60 mm) + 0.35·flood_hazard_true + 0.15·clamp(true_incidents_6h / 4)
       + 0.10·true_traffic_disruption + ε,   ε ~ N(0, 0.07)
label  = Low < 0.28 ≤ Moderate < 0.60 ≤ High < 0.84 ≤ Critical
```

The thresholds sit near the 35th/65th/87th percentiles of `latent` in the
default 60-day backfill (seed 42), giving an imbalanced but complete label set
(Low 634, Moderate 454, High 413, Critical 331 incidents as generated). The
model never sees these latent values; it sees ETL features computed from
synthetic *sensor observations* (rain measured with ±8 % noise at the nearest
station, river gauges only within 6 km, traffic only where segments exist,
public reports as a noisy proxy of recent incidents). It can therefore learn
an approximation of the rule — this is stated as a limitation, not a result.

## Features (point-in-time, no leakage)

Built by the batch ETL (`spark_jobs/features.py`) for every archived
flood-related incident with a zone; only data from hourly buckets **before the
incident's own hour** and reports **strictly before** the incident time are used.

| Feature | Unit | Definition | Missing handling |
|---|---|---|---|
| rain_3h_mm | mm | Σ mean rainfall intensity over the 3 previous hours (nearest station ≤ 12 km) | median imputation |
| rain_max_1h_mm_hr | mm/hr | peak intensity in the previous hour | median |
| level_ratio_max_3h | ratio | max level / danger level over 3 h (nearest gauge ≤ 6 km) | median + `water_obs_available` |
| humidity_avg_3h | % | mean relative humidity, 3 h | median |
| disrupted_ratio_1h | 0–1 | share of disrupted traffic observations in the zone, previous hour | median + `traffic_obs_available` |
| speed_ratio_avg_1h | 0–1 | mean speed / free-flow speed, previous hour | median |
| reports_6h | count | non-rejected public reports in the zone in the previous 6 h | 0 if none (observed count) |
| water_obs_available | 0/1 | any gauge reading in the 3 h window | — |
| traffic_obs_available | 0/1 | any traffic reading in the previous hour | — |

Excluded on purpose (`FORBIDDEN_FEATURES` in `model_training.py`): the label,
`severity`, incident id, category, zone id, timestamps and the archived
incident counts. The training code refuses to run if any of them is
configured as a feature, and the API rejects a `severity` field in prediction requests.

## Methodology

1. Load `gold/ml_training_flood_severity`; validate schema, numeric types,
   non-null labels, allowed labels and minimum rows (`model.min_rows`).
2. Index labels with `StringIndexerModel.from_labels` (fixed mapping).
3. **Time-based split:** the most recent 20 % of incidents (by event time) is
   the test set (`approxQuantile` cut-off); training rows are all earlier.
4. `Pipeline([Imputer(median), VectorAssembler, DecisionTreeClassifier, IndexToString])`
   fitted on the training split only (so imputation medians never see test data).
   Parameters: `maxDepth=6, minInstancesPerNode=5, impurity=gini, seed=42`.
5. Predict the held-out split; compute accuracy, weighted precision/recall/F1
   (`MulticlassClassificationEvaluator`), per-class precision/recall/F1
   (`precisionByLabel` etc.), the confusion matrix, and a majority-class baseline.
6. Save to `data/models/flood_severity_dt/<version>/pipeline`, write
   `metadata.json` (features, labels, provenance, split, metrics, tree depth,
   feature importances, debug string) and `latest.json`.
7. Reload the saved model and check it reproduces the in-memory predictions on 25 test rows.

Commands: `scripts\train_model.ps1` (or
`docker compose run --rm --no-deps tools python -m spark_jobs.model_training`).
The machine-readable report is `data/reports/ml_evaluation.json`.

## Inference and integration

* `SeverityModel` (`spark_jobs/model_inference.py`) loads the PipelineModel
  and predicts validated feature records (`aegis/ml_contract.py`).
* `python -m spark_jobs.model_inference --zones` computes the same features
  for every zone from the streamed silver tables and writes predictions to
  `lake/live/ml_predictions` and HBase `model_predictions` (reverse-timestamp keys).
  The live demo stream is time-compressed (one 6-hour storm in 20 minutes), so
  live features use 200-second buckets that each represent one simulated hour
  (`model.live_bucket_seconds`). Features are computed as of *now*; with
  `--as-of-latest-event` they are computed as of the end of the newest data
  bucket instead (used by the end-to-end check, which pauses the live services).
  The time used is recorded as `features_as_of`.
* The API hosts the model in its own local Spark session; `POST /api/model/predict`
  validates the input, predicts, and persists the request and result to HBase.
* The dashboard shows the prediction separately from the rule-based zone score
  and from observed (archived, synthetic) severities.
* The model is optional: ingestion, ETL, streaming, HBase writes and the
  rule-based risk score never depend on it; without a trained model the API
  returns 503 for predictions and everything else keeps working.

## Results

Results are written by the training run to `data/reports/ml_evaluation.json`
and summarised in the final project report. See the section "Actual
evaluation results" below for the values produced in this workspace.

### Actual evaluation results

Measured on 2026-10-21 in the `aegis-spark:3.5.9` container
(`python -m spark_jobs.model_training`, `scripts\train_model.ps1` and
`scripts\run_e2e.ps1`; all six training runs, model versions
`20261021T101649Z` … `20261021T151653Z`, gave identical numbers). Data: the
default 60-day backfill (seed 42; end dates 2026-10-20 and 2026-10-21 give the
same content shifted in time). **Labels are synthetic.**

| Item | Value |
|---|---|
| Training set | 1,822 archived flood-related incidents (Low 628 · Moderate 452 · High 411 · Critical 331) |
| Split | time-based, cut-off 2026-10-10 02:41 UTC → train 1,457 / test 365 |
| Test distribution | Low 118 · Moderate 87 · High 75 · Critical 85 |
| Accuracy | **0.677** |
| Weighted precision / recall / F1 | 0.658 / 0.677 / **0.664** |
| Majority-class baseline (always "Low") | 0.323 accuracy |
| Tree | depth 6, 69 nodes; fit 2.1 s |
| Save → reload check | passed (25 test rows, identical predictions) |

Per class (test split):

| Class | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| Low | 0.861 | 0.890 | 0.875 | 118 |
| Moderate | 0.521 | 0.563 | 0.541 | 87 |
| High | 0.412 | 0.280 | 0.333 | 75 |
| Critical | 0.735 | 0.847 | 0.787 | 85 |

Confusion matrix (rows = actual, columns = predicted; order Low, Moderate, High, Critical):

```
            Low  Mod  High  Crit
Low         105   11     2     0
Moderate     16   49    19     3
High          1   30    21    23
Critical      0    4     9    72
```

Feature importances: rain_3h_mm 0.713 · reports_6h 0.118 · rain_max_1h_mm_hr 0.053 ·
speed_ratio_avg_1h 0.049 · level_ratio_max_3h 0.049 · disrupted_ratio_1h 0.017 ·
water_obs_available 0.003 · humidity_avg_3h 0 · traffic_obs_available 0.

Missing values in the training data (imputed with training medians):
level_ratio_max_3h 723 rows (zones without a gauge within 6 km),
disrupted_ratio_1h / speed_ratio_avg_1h 634 rows (no traffic segment), rain features 1 row.

**Interpretation.** The tree clearly beats the majority baseline (0.68 vs 0.32)
and separates the extremes well (Low and Critical F1 0.88 / 0.79). The middle
classes are hard: "High" is mostly confused with Moderate and Critical (recall
0.28), as expected when the synthetic label comes from a continuous latent
score cut at fixed thresholds and the model only sees noisy proxies of it.
Rainfall dominates, mirroring the label rule's largest weight. These numbers
describe recovery of a known synthetic rule, not real-world skill.

### Zone predictions without weather data

Rainfall features are almost never missing in training (1 of 1,822 rows), so
imputing them would use the training median — a "rainy" value — and invent a
severity. During verification, an empty live window made every zone predict
"Moderate" this way. Per-zone scoring (`--zones`) therefore returns
**no prediction** (`predicted_severity = null`,
`prediction_status = "insufficient_data: no weather observation in the feature window"`)
for zones whose rainfall features are missing; the dashboard shows
"Insufficient data". Single-record API predictions still impute and list
`imputed_features` in the response.

## Limitations

* Synthetic labels from a known rule; the model may simply learn that rule.
* One time-based split of simulated data; no confidence intervals.
* Class imbalance (Critical is the rarest class); weighted metrics and per-class
  scores are both reported.
* Zones without gauges or traffic segments rely on imputed values (flagged by
  the availability features and listed in `imputed_features` for API predictions).
* Live predictions use rescaled windows on time-compressed demo data and are illustrative only.
* The numbers above are for the backfill alone. When the batch ETL also
  ingests archived live-stream records whose wall-clock times overlap the end
  of the backfill, the features of the newest incidents change and so do the
  metrics (2026-10-22 e2e run: accuracy 0.660, weighted F1 0.643).
  Regenerating the backfill (`scripts\generate_data.ps1`) after streaming has
  run creates such an overlap.

## Hybrid-mode model (2026-10-22)

`scripts\run_hybrid_data.ps1` trains the same pipeline on `data_hybrid/`:
real ERA5 rainfall as the weather features, and **synthetic** water level,
traffic, reports, incidents and labels simulated from that rainfall (see
[dataset_migration_plan.md](dataset_migration_plan.md) §5b). Labels still come
from `severity_rule`, so they are synthetic.

| Item | Value |
|---|---|
| Training set | 1,549 flood-related incidents (Low 490, Moderate 482, High 377, Critical 200) |
| Split | time-based, cutoff 2023-12-05 06:47 UTC: 1,239 train / 310 test |
| Test labels | Low 224, Moderate 85, High 1, **Critical 0** |
| Accuracy / weighted F1 | 0.842 / 0.845 (majority baseline 0.274) |
| Per-class F1 | Low 0.888, Moderate 0.741, High 0.0 (1 example), Critical not measurable |

The period holds one cyclone, so a leakage-free time split puts almost all
severe incidents in training. The score therefore says little about High or
Critical cases and must not be compared with the synthetic-mode model. Per-class
scores of a class absent from the test set are reported as `null` (support 0);
earlier the evaluation crashed in that case.

## Real-data audit (2026-10-22)

Checked against every real source verified for the migration
([dataset_migration_plan.md](dataset_migration_plan.md) §1 and §4).

| Question | Finding |
|---|---|
| Is there a real label for this objective? | **No.** No verified open source gives Low/Moderate/High/Critical severity per incident or per zone-hour for Chennai |
| Could DFO or GDACS serve as labels? | No. DFO `SEVERITY` (1 / 1.5 / 2) is one value per regional event (12 events since 1989 intersect the study box; each polygon covers all 40 zones at once). GDACS alert levels are per cyclone/flood event. Using either as a zone-level incident label would fabricate spatial detail |
| Could the model be trained on real inputs with the synthetic labels? | No. The labels are computed from the simulator's hidden state; they do not exist for real days |
| What happens in real-data mode? | The ETL builds **no** training set (0 rows; the "ML training set" data-quality check is reported as *not applicable*), the real-mode API runs with the model off (`AEGIS_API_MODEL=0`), and the dashboard's ML tab says the model is unavailable. Nothing synthetic is shown as a real prediction |
| Do the existing metrics stay valid? | Only as a synthetic-label demonstration (accuracy 0.677 above). They say nothing about real-world skill |
| A defensible real-data objective | Predict a zone's **IMD rainfall category for the next 24 h** (IMD's published classes: no rain < 2.5 mm, light 2.5–15.5, moderate 15.6–64.4, heavy 64.5–115.5, very heavy 115.6–204.4, extremely heavy ≥ 204.5 mm) from past ERA5/IMD values. This is a **proxy target derived from observed measurements with official thresholds**, not a flood-severity label, and it changes the model's objective. **Not implemented** — it needs the project owner's approval |
| Data size for that objective | 60 days × 40 zones = 2,400 zone-days from only 3 IMD cells (effectively ~180 independent cell-days, one cyclone). A credible model would need several monsoon seasons (IMD offers 1901–2025) |
