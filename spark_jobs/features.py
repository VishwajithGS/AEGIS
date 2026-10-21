"""Point-in-time feature engineering for the flood-severity classifier.

Features are computed from per-zone time buckets (``aggregate_signals``)
using ONLY data from buckets that end at or before the start of the target's
own bucket, plus public reports strictly before the target time. Nothing from
the target incident itself (its severity, its own reports, later sensor
readings) can enter the features.

A bucket represents one hour of observed time. For the time-accelerated live
demo stream the inference job passes a shorter ``bucket_seconds`` so that a
bucket still represents one simulated hour (see docs/ml_model.md).

=====================  ===========  ==========================================
feature                unit         definition
=====================  ===========  ==========================================
rain_3h_mm             mm           sum of mean rainfall intensity over the 3
                                    buckets before the target bucket (x 1 h)
rain_max_1h_mm_hr      mm/hr        max rainfall intensity in the previous bucket
level_ratio_max_3h     ratio        max water level / danger level, 3 buckets
humidity_avg_3h        %            mean relative humidity, 3 buckets
disrupted_ratio_1h     0-1          share of disrupted traffic obs, previous bucket
speed_ratio_avg_1h     0-1          mean speed / free-flow speed, previous bucket
reports_6h             count        non-rejected public reports in the zone in
                                    the 6 buckets before the target time
water_obs_available    0/1          any gauge reading in the 3 buckets
traffic_obs_available  0/1          any traffic reading in the previous bucket
=====================  ===========  ==========================================

Missing numeric values stay NULL here; the model pipeline imputes them with
training-set medians (Spark ``Imputer``) and the two availability flags keep
the missingness visible to the model.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from aegis.ml_contract import FEATURE_COLUMNS  # noqa: F401 (re-exported)



def _ago(col: str, seconds: int):
    return F.col(col) - F.expr(f"INTERVAL {int(seconds)} SECONDS")


def point_in_time_features(targets: DataFrame, buckets: DataFrame, reports: DataFrame,
                           bucket_seconds: int = 3600) -> DataFrame:
    """``targets(target_id, zone_id, t)``; ``buckets(zone_id, window_start, ...)``
    from ``aggregate_signals``; ``reports(zone_id, event_time)`` of non-rejected
    public reports. Returns targets with the feature columns appended."""
    bs = int(bucket_seconds)
    tg = targets.withColumn("t_bucket", F.timestamp_seconds(F.floor(F.unix_timestamp("t") / bs) * bs))
    b = buckets.select(
        F.col("zone_id").alias("b_zone"), F.col("window_start").alias("b_start"),
        "rainfall_avg_mm_hr", "rainfall_max_mm_hr", "humidity_avg", "level_ratio_max", "water_obs",
        "disrupted_ratio", "speed_ratio_avg", "traffic_obs")

    three = (tg.join(b, (tg.zone_id == b.b_zone) & (b.b_start >= _ago("t_bucket", 3 * bs))
                     & (b.b_start < F.col("t_bucket")), "left")
             .groupBy("target_id")
             .agg(F.round(F.sum("rainfall_avg_mm_hr"), 3).alias("rain_3h_mm"),
                  F.round(F.max("level_ratio_max"), 4).alias("level_ratio_max_3h"),
                  F.round(F.avg("humidity_avg"), 2).alias("humidity_avg_3h"),
                  F.coalesce(F.sum("water_obs"), F.lit(0)).alias("_water_obs")))
    one = (tg.join(b, (tg.zone_id == b.b_zone) & (b.b_start == _ago("t_bucket", bs)), "left")
           .groupBy("target_id")
           .agg(F.round(F.max("rainfall_max_mm_hr"), 3).alias("rain_max_1h_mm_hr"),
                F.round(F.avg("disrupted_ratio"), 4).alias("disrupted_ratio_1h"),
                F.round(F.avg("speed_ratio_avg"), 4).alias("speed_ratio_avg_1h"),
                F.coalesce(F.sum("traffic_obs"), F.lit(0)).alias("_traffic_obs")))
    r = reports.select(F.col("zone_id").alias("r_zone"), F.col("event_time").alias("r_time"))
    rep = (tg.join(r, (tg.zone_id == r.r_zone) & (r.r_time >= _ago("t", 6 * bs)) & (r.r_time < F.col("t")), "left")
           .groupBy("target_id").agg(F.count("r_time").alias("reports_6h")))

    return (tg.join(three, "target_id", "left").join(one, "target_id", "left").join(rep, "target_id", "left")
            .withColumn("reports_6h", F.coalesce(F.col("reports_6h"), F.lit(0)).cast("double"))
            .withColumn("water_obs_available", (F.col("_water_obs") > 0).cast("double"))
            .withColumn("traffic_obs_available", (F.col("_traffic_obs") > 0).cast("double"))
            .drop("_water_obs", "_traffic_obs", "t_bucket"))
