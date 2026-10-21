"""AEGIS real-time pipeline (Spark Structured Streaming over Kafka).

Three queries read the four source topics; each has its own checkpoint:

1. ``bronze_dq``  (stateless, foreachBatch)
   raw Kafka records -> ``lake/bronze/kafka_raw`` (topic/ingest_date partitions);
   parse + validate -> rejected records to ``lake/quarantine/stream``;
   per-batch DQ metrics -> ``lake/live/dq_metrics``, HBase ``processing_metrics``
   and the ``data-quality-events`` topic.
2. ``silver``     valid records -> watermark -> ``dropDuplicatesWithinWatermark(event_id)``
   -> ``lake/silver_stream/<domain>`` + HBase ``sensor_observations`` / ``incident_records``
   (write count checked against the batch, sample rows read back).
3. ``zone_risk``  valid records -> watermark -> dedup -> zone signals (stream-static
   join with the nearest-station map) -> tumbling event-time windows per zone
   (output mode *update*) -> rule-based risk score -> HBase ``zone_risk_history``,
   ``lake/live/zone_risk`` and the ``zone-risk-updates`` topic.

Late data: the watermark is ``streaming.watermark`` (default 10 minutes) behind
the maximum event time seen. Records older than the watermark are dropped by
the stateful operators of queries 2 and 3 (they are NOT added to live windows),
but query 1 still archives them in bronze and flags them ``late_arrival``, so
the batch ETL includes them in the historical (batch) layer.

Guarantees: Kafka offsets are tracked in the checkpoints; a failed micro-batch
is re-executed after restart (at-least-once into the sinks). HBase puts are
idempotent by row key; Parquet appends are guarded by per-batch marker files
so a replayed batch is not appended twice. No end-to-end exactly-once claim
is made.

    python -m spark_jobs.streaming_pipeline                    # run until stopped
    python -m spark_jobs.streaming_pipeline --available-now    # drain Kafka, then stop
    python -m spark_jobs.streaming_pipeline --run-seconds 300
Stop gracefully with Ctrl+C / SIGTERM or by creating data/state/streaming/STOP.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from aegis.config import data_path, get_config, source_topics, topic
from aegis.logging_config import setup_logging
from aegis.storage.hbase_repository import HBaseRepository, HBaseUnavailable, metric_row_key
from spark_jobs import hbase_sink, transforms as TR
from spark_jobs.common import DOMAINS, build_spark, checkpoint, lake, path_exists, remove_dir, write_json
from spark_jobs.risk_analytics import score, signal_aggregations, zone_signals
from spark_jobs.spatial_analytics import nearest_station_map

log = setup_logging("streaming")
QUERIES = ("bronze_dq", "silver", "zone_risk")
RESTART_RESET_S = 600          # a query healthy this long after a restart gets its restart budget back


def restart_delay_s(attempt: int) -> int:
    """Back-off between automatic query restarts: 10, 20, 40, 60, 60 ... seconds, so a
    dependency outage of a few minutes (e.g. HBase restarting) is ridden out."""
    return min(60, 10 * 2 ** (attempt - 1))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StatusBoard:
    """Thread-safe status document at data/state/streaming/status.json (read by the API)."""

    def __init__(self, cfg: dict, mode: str):
        self.dir = data_path(cfg, "state_dir", "streaming")
        self.dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.doc = {"started_at": _now_iso(), "mode": mode, "pid": os.getpid(), "state": "starting",
                    "kafka_bootstrap": cfg["kafka"]["bootstrap_servers"], "queries": {}, "hbase_sink": "unknown",
                    "watermark": cfg["streaming"]["watermark"], "window": cfg["streaming"]["window_duration"]}

    def update(self, **kw) -> None:
        with self.lock:
            self.doc.update(kw)
            self.doc["updated_at"] = _now_iso()
            write_json(self.dir / "status.json", self.doc)

    def query(self, name: str, **kw) -> None:
        with self.lock:
            q = self.doc["queries"].setdefault(name, {})
            q.update(kw)
            self.doc["updated_at"] = _now_iso()
            write_json(self.dir / "status.json", self.doc)

    def append_progress(self, rec: dict) -> None:
        p = self.dir / "progress.jsonl"
        with self.lock:
            if p.exists() and p.stat().st_size > 5_000_000:
                p.replace(self.dir / "progress.1.jsonl")
            with p.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, default=str) + "\n")


def kafka_records_in_batch(sources: list) -> Optional[int]:
    """Exact number of Kafka records in a micro-batch: Σ(endOffset − startOffset) over all
    topic partitions of all sources. None when it cannot be computed (no start offsets yet)."""
    total, known = 0, False
    for s in sources or []:
        start, end = s.get("startOffset"), s.get("endOffset")
        try:
            start = json.loads(start) if isinstance(start, str) else start
            end = json.loads(end) if isinstance(end, str) else end
        except ValueError:
            continue
        if not isinstance(end, dict) or not isinstance(start, dict):
            continue
        for topic_name, parts in end.items():
            for part, e in (parts or {}).items():
                s0 = (start.get(topic_name) or {}).get(part)
                if isinstance(s0, int) and isinstance(e, int) and s0 >= 0 and e >= s0:
                    total += e - s0
                    known = True
    return total if known else None


class ProgressListener(StreamingQueryListener):
    def __init__(self, board: StatusBoard):
        self.board = board
        self.names: dict[str, str] = {}      # query id -> name (idle/terminated events carry only the id)

    def _name(self, event) -> str:
        return self.names.get(str(event.id), str(event.id))

    def onQueryStarted(self, event):
        name = event.name or str(event.id)
        self.names[str(event.id)] = name
        self.board.query(name, id=str(event.id), run_id=str(event.runId), status="active", started_at=_now_iso())

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        name = p.get("name") or p.get("id")
        # Spark's numInputRows counts the Kafka source once per plan branch that scans it
        # (silver splits by domain, zone_risk also by signal), so it overstates ingest.
        # The exact number of Kafka records in the batch comes from the offset ranges.
        scanned = p.get("numInputRows") or 0
        records = kafka_records_in_batch(p.get("sources", []))
        scale = (records / scanned) if (records is not None and scanned) else None
        rate = lambda v: round(v * scale, 3) if (v is not None and scale is not None) else None  # noqa: E731
        summary = {
            "batch_id": p.get("batchId"), "timestamp": p.get("timestamp"),
            "kafka_records": records, "rows_scanned": scanned,
            "num_input_rows": records if records is not None else scanned,
            "input_rows_per_second": rate(p.get("inputRowsPerSecond")),
            "processed_rows_per_second": rate(p.get("processedRowsPerSecond")),
            "trigger_ms": (p.get("durationMs") or {}).get("triggerExecution"),
            "watermark": (p.get("eventTime") or {}).get("watermark"),
            "state_rows": sum(int(s.get("numRowsTotal", 0)) for s in p.get("stateOperators", [])),
            "dropped_by_watermark": sum(int(s.get("numRowsDroppedByWatermark", 0)) for s in p.get("stateOperators", [])),
            "end_offsets": [s.get("endOffset") for s in p.get("sources", [])],
        }
        self.board.query(name, status="active", last_progress=summary)
        self.board.append_progress({"query": name, **summary})

    def onQueryIdle(self, event):
        self.board.query(self._name(event), idle_since=_now_iso())

    def onQueryTerminated(self, event):
        self.board.query(self._name(event), status="failed" if event.exception else "terminated",
                         terminated_at=_now_iso(), exception=(event.exception or None))


class BatchMarkers:
    """Per-query batch markers that make Parquet appends idempotent on replay."""

    def __init__(self, cfg: dict, query: str):
        self.dir = Path(checkpoint(cfg, query)) / "_aegis_batches"
        self.dir.mkdir(parents=True, exist_ok=True)

    def done(self, batch_id: int) -> bool:
        return (self.dir / f"{batch_id}.done").exists()

    def mark(self, batch_id: int, info: dict) -> None:
        (self.dir / f"{batch_id}.done").write_text(json.dumps(info, default=str), encoding="utf-8")


class Pipeline:
    def __init__(self, spark: SparkSession, cfg: dict, args: argparse.Namespace):
        self.spark, self.cfg, self.args = spark, cfg, args
        self.board = StatusBoard(cfg, "available-now" if args.available_now else "continuous")
        self.bootstrap = cfg["kafka"]["bootstrap_servers"]
        self.hbase = None if args.no_hbase else hbase_sink.hbase_settings(cfg)
        self.fail_on_hbase_error = bool(cfg["hbase"].get("fail_on_error", True))
        self.max_chars = int(cfg["streaming"]["max_raw_value_chars"])
        self.wm = cfg["streaming"]["watermark"]
        self.queries: dict[str, StreamingQuery] = {}
        self.restarts: dict[str, int] = {q: 0 for q in QUERIES}
        self.last_restart: dict[str, float] = {}
        self.stop_requested = False
        self.hazard = None
        self.wmap = self.lmap = None
        self.latest_path = data_path(cfg, "state_dir", "zone_risk_latest.json")

    # -- setup --------------------------------------------------------------
    def check_hbase(self) -> None:
        if self.hbase is None:
            self.board.update(hbase_sink="disabled (--no-hbase or hbase.enabled=false)")
            return
        try:
            with HBaseRepository.from_config(self.cfg) as repo:
                created = repo.ensure_tables()
            self.board.update(hbase_sink=f"enabled ({self.hbase['host']}:{self.hbase['port']})", hbase_tables=created)
        except HBaseUnavailable as exc:
            if self.args.require_hbase:
                raise SystemExit(f"HBase required but unavailable: {exc}")
            log.error(f"HBase unavailable at startup - HBase sink DISABLED for this run, Parquet sinks continue: {exc}")
            self.board.update(hbase_sink=f"disabled: unreachable at startup ({exc})")
            self.hbase = None

    def load_static(self) -> None:
        self.wmap = nearest_station_map(self.spark, self.cfg, "weather")
        self.lmap = nearest_station_map(self.spark, self.cfg, "water_level")
        hz_path = lake(self.cfg, "gold", "zone_hazard_history")
        if path_exists(hz_path):
            self.hazard = self.spark.read.parquet(hz_path).select("zone_id", "historical_hazard_index").cache()
            log.info(f"historical hazard index loaded for {self.hazard.count()} zones")
        else:
            log.warning("gold/zone_hazard_history missing (run the batch ETL); historical indicator will be missing")

    def kafka_source(self) -> DataFrame:
        s = self.cfg["streaming"]
        df = (self.spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap)
              .option("subscribe", ",".join(source_topics(self.cfg)))
              .option("startingOffsets", self.args.starting_offsets or s["starting_offsets"])
              .option("maxOffsetsPerTrigger", int(s["max_offsets_per_trigger"]))
              .option("failOnDataLoss", "false")
              .load())
        return df.select(
            F.col("value").cast("string").alias("value"), "topic",
            F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("ingest_time"), F.lit("kafka").alias("origin"),
            F.col("offset").alias("source_seq"), F.col("key").cast("string").alias("key"))

    def _writer(self, df: DataFrame, name: str, fn: Callable, output_mode: str = "append"):
        w = (df.writeStream.queryName(name).outputMode(output_mode).foreachBatch(fn)
             .option("checkpointLocation", checkpoint(self.cfg, name)))
        if self.args.available_now:
            return w.trigger(availableNow=True)
        return w.trigger(processingTime=f"{int(self.cfg['streaming']['trigger_seconds'])} seconds")

    # -- sinks --------------------------------------------------------------
    def _put_metric(self, component: str, metrics: dict, status: dict) -> None:
        if self.hbase is None:
            return
        try:
            with HBaseRepository.from_config(self.cfg) as repo:
                repo.put("processing_metrics", metric_row_key(component, int(time.time() * 1000)),
                         {"metrics": metrics, "status": status})
        except HBaseUnavailable as exc:
            log.error(f"processing_metrics write failed: {exc}")
            if self.fail_on_hbase_error:
                raise

    def bronze_dq_batch(self, df: DataFrame, batch_id: int) -> None:
        markers = BatchMarkers(self.cfg, "bronze_dq")
        if markers.done(batch_id):
            log.info(f"bronze_dq batch {batch_id} already committed; skipping replay")
            return
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            n = df.count()
            if n == 0:
                return
            t0 = time.time()
            (df.withColumn("ingest_date", F.to_date("ingest_time"))
             .select("topic", "kafka_partition", "kafka_offset", F.col("ingest_time").alias("kafka_timestamp"),
                     "key", "value", "ingest_date")
             .coalesce(1).write.mode("append").partitionBy("topic", "ingest_date").parquet(lake(self.cfg, "bronze", "kafka_raw")))
            canon = TR.union_canonical(TR.canonicalize_all(df, self.cfg).values()).persist(StorageLevel.MEMORY_AND_DISK)
            (TR.quarantine_view(canon, self.max_chars).coalesce(1).write.mode("append").partitionBy("quarantine_date")
             .parquet(lake(self.cfg, "quarantine", "stream")))
            totals = {r["domain"]: {"records": r["records"], "valid": r["valid"], "rejected": r["rejected"]}
                      for r in TR.dq_counts(canon).collect()}
            reasons = {f"{r['domain']}.{r['reason']}": r["count"] for r in TR.reason_counts(canon).collect()}
            flags = {f"{r['domain']}.{r['flag']}": r["count"] for r in TR.flag_counts(canon).collect()}
            canon.unpersist()
            rec = {"batch_id": batch_id, "processed_at": _now_iso(), "records": n,
                   "valid": sum(v["valid"] for v in totals.values()),
                   "rejected": sum(v["rejected"] for v in totals.values()),
                   "by_domain": totals, "rejected_by_reason": reasons, "flags": flags,
                   "duration_s": round(time.time() - t0, 3)}
            self.spark.createDataFrame([(batch_id, rec["processed_at"], n, rec["valid"], rec["rejected"],
                                         json.dumps(rec))],
                                       "batch_id long, processed_at string, records long, valid long, "
                                       "rejected long, detail_json string") \
                .write.mode("append").parquet(lake(self.cfg, "live", "dq_metrics"))
            self.spark.createDataFrame([("bronze_dq", json.dumps(rec))], "key string, value string") \
                .write.format("kafka").option("kafka.bootstrap.servers", self.bootstrap) \
                .option("topic", topic(self.cfg, "data_quality")).save()
            self._put_metric("stream.bronze_dq", {"records": n, "valid": rec["valid"], "rejected": rec["rejected"],
                                                  "duration_s": rec["duration_s"], "batch_id": batch_id},
                             {"state": "ok", "detail": json.dumps({"reasons": reasons, "flags": flags})})
            self.board.update(last_dq=rec)
            markers.mark(batch_id, {"records": n})
        finally:
            df.unpersist()

    def silver_batch(self, df: DataFrame, batch_id: int) -> None:
        markers = BatchMarkers(self.cfg, "silver")
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            counts = {r["domain"]: r["count"] for r in df.groupBy("domain").count().collect()}
            if not counts:
                return
            if not markers.done(batch_id):
                for d in DOMAINS:
                    if counts.get(d):
                        (TR.domain_view(df, d).coalesce(1).write.mode("append").partitionBy("event_date")
                         .parquet(lake(self.cfg, "silver_stream", d)))
            hb: dict = {}
            if self.hbase is not None:
                try:
                    sensors = hbase_sink.prepare_sensor(df.filter(F.col("domain") != "incidents"))
                    incidents = hbase_sink.prepare_incident(df.filter(F.col("domain") == "incidents"))
                    expected_s = sum(v for k, v in counts.items() if k != "incidents")
                    expected_i = counts.get("incidents", 0)
                    hb["sensor_written"] = hbase_sink.write(sensors, "sensor_observations", hbase_sink.SENSOR_SPEC,
                                                            hbase_sink.sensor_key, self.hbase) if expected_s else 0
                    hb["incident_written"] = hbase_sink.write(incidents, "incident_records", hbase_sink.INCIDENT_SPEC,
                                                              hbase_sink.incident_key, self.hbase) if expected_i else 0
                    if hb["sensor_written"] != expected_s or hb["incident_written"] != expected_i:
                        raise HBaseUnavailable(f"HBase write count mismatch: {hb} expected sensors={expected_s} "
                                               f"incidents={expected_i}")
                    if expected_s:
                        hb["verify"] = hbase_sink.verify_sample(sensors, "sensor_observations", hbase_sink.sensor_key,
                                                                self.hbase, "metadata", "event_id", n=2)
                except HBaseUnavailable as exc:
                    log.error(f"silver batch {batch_id}: HBase write failed: {exc}")
                    hb["error"] = str(exc)
                    if self.fail_on_hbase_error:
                        raise
            self._put_metric("stream.silver", {"batch_id": batch_id, **{f"rows_{k}": v for k, v in counts.items()}},
                             {"state": "error" if "error" in hb else "ok", "detail": json.dumps(hb, default=str)})
            self.board.update(last_silver={"batch_id": batch_id, "rows": counts, "hbase": hb, "at": _now_iso()})
            markers.mark(batch_id, {"rows": counts})
        finally:
            df.unpersist()

    def zone_risk_batch(self, df: DataFrame, batch_id: int) -> None:
        markers = BatchMarkers(self.cfg, "zone_risk")
        if self.hazard is not None:
            df = df.join(F.broadcast(self.hazard), "zone_id", "left")
        scored = (score(df, self.cfg)
                  .withColumn("assessed_at", F.current_timestamp())
                  .withColumn("data_origin", F.lit("live-stream"))
                  .withColumn("batch_id", F.lit(batch_id).cast("long"))
                  .withColumn("window_date", F.to_date("window_start"))).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            rows = scored.count()
            if rows == 0:
                return
            if not markers.done(batch_id):
                scored.coalesce(1).write.mode("append").partitionBy("window_date").parquet(lake(self.cfg, "live", "zone_risk"))
            if self.hbase is not None:
                try:
                    zr = hbase_sink.prepare_zone_risk(scored)
                    written = hbase_sink.write(zr, "zone_risk_history", hbase_sink.ZONE_RISK_SPEC,
                                               hbase_sink.zone_risk_key, self.hbase)
                    if written != rows:
                        raise HBaseUnavailable(f"zone_risk_history wrote {written} of {rows} rows")
                except HBaseUnavailable as exc:
                    log.error(f"zone_risk batch {batch_id}: HBase write failed: {exc}")
                    if self.fail_on_hbase_error:
                        raise
            cols = ["zone_id", "window_start", "window_end", "risk_score", "risk_category", "confidence", "coverage",
                    "explanation", "contributions", "available_features", "missing_features", "rainfall_avg_mm_hr",
                    "level_ratio_max", "weighted_reports", "reports", "disrupted_ratio", "historical_hazard_index",
                    "weather_obs", "water_obs", "traffic_obs", "assessed_at", "data_origin", "batch_id"]
            present = [c for c in cols if c in scored.columns]
            # Timestamps are rendered as UTC ISO strings inside Spark: collect() would otherwise
            # turn them into naive datetimes in the driver's local time zone.
            ts_cols = {"window_start", "window_end", "assessed_at"}
            out = scored.select(*[hbase_sink.iso(c).alias(c) if c in ts_cols else F.col(c) for c in present])
            (out.select(F.col("zone_id").alias("key"), F.to_json(F.struct(*present)).alias("value"))
             .write.format("kafka").option("kafka.bootstrap.servers", self.bootstrap)
             .option("topic", topic(self.cfg, "zone_risk")).save())
            self._update_latest([r.asDict() for r in out.collect()])   # <= zones x open windows
            self._put_metric("stream.zone_risk", {"batch_id": batch_id, "updated_zone_windows": rows},
                             {"state": "ok"})
            markers.mark(batch_id, {"rows": rows})
        finally:
            scored.unpersist()

    def _update_latest(self, rows: list[dict]) -> None:
        """Latest assessment per zone (newest window wins) for API fallback and staleness checks."""
        doc = {}
        if self.latest_path.exists():
            try:
                doc = json.loads(self.latest_path.read_text(encoding="utf-8")).get("zones", {})
            except ValueError:
                doc = {}
        for r in rows:
            cur = doc.get(r["zone_id"])
            if cur is None or str(r["window_start"]) >= str(cur["window_start"]):
                doc[r["zone_id"]] = r
        write_json(self.latest_path, {"updated_at": _now_iso(), "data_origin": "live-stream", "zones": doc})

    # -- queries ------------------------------------------------------------
    def start_query(self, name: str) -> StreamingQuery:
        src = self.kafka_source()
        if name == "bronze_dq":
            q = self._writer(src, name, self.bronze_dq_batch).start()
        else:
            valid = TR.union_canonical(TR.canonicalize_all(src, self.cfg).values()).filter(F.col("is_valid"))
            dedup = valid.withWatermark("event_time", self.wm).dropDuplicatesWithinWatermark(["event_id"])
            if name == "silver":
                q = self._writer(dedup, name, self.silver_batch).start()
            else:
                agg = (zone_signals(dedup, self.wmap, self.lmap)
                       .groupBy(F.window("event_time", self.cfg["streaming"]["window_duration"]).alias("w"), "zone_id")
                       .agg(*signal_aggregations())
                       .select("zone_id", F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                               *[c for c in ("rainfall_avg_mm_hr", "rainfall_max_mm_hr", "weather_obs", "humidity_avg",
                                             "level_ratio_max", "water_obs", "disrupted_ratio", "speed_ratio_avg",
                                             "traffic_obs", "weighted_reports", "reports", "flood_reports",
                                             "archived_incidents", "archived_flood_incidents", "signal_count")]))
                q = self._writer(agg, name, self.zone_risk_batch, output_mode="update").start()
        self.queries[name] = q
        self.board.query(name, id=str(q.id), status="active", restarts=self.restarts[name])
        log.info(f"started query {name} id={q.id}")
        return q

    def request_stop(self, *_):
        self.stop_requested = True

    def run(self) -> int:
        if self.args.reset:
            for q in QUERIES:
                remove_dir(checkpoint(self.cfg, q))
            log.warning("checkpoints removed (--reset); queries restart from streaming.starting_offsets")
        self.check_hbase()
        self.load_static()
        self.spark.streams.addListener(ProgressListener(self.board))
        for name in self.args.queries:
            self.start_query(name)
        self.board.update(state="running", queries_requested=self.args.queries)
        stop_file = data_path(self.cfg, "state_dir", "streaming", "STOP")
        deadline = time.time() + self.args.run_seconds if self.args.run_seconds else None
        exit_code = 0
        while True:
            if stop_file.exists():
                stop_file.unlink()
                log.info("STOP file found")
                self.stop_requested = True
            if self.stop_requested or (deadline and time.time() >= deadline):
                break
            active = {n: q for n, q in self.queries.items() if q.isActive}
            for name, q in list(self.queries.items()):
                if q.isActive:
                    # healthy for RESTART_RESET_S since the last restart -> forget past failures
                    if self.restarts[name] and time.time() - self.last_restart.get(name, 0) > RESTART_RESET_S:
                        log.info(f"query {name} healthy for {RESTART_RESET_S} s; restart counter reset")
                        self.restarts[name] = 0
                        self.board.query(name, restarts=0)
                    continue
                exc = q.exception()
                if exc is None:
                    continue           # finished normally (available-now)
                msg = str(exc).splitlines()[0][:500]
                log.error(f"query {name} failed: {msg}")
                self.board.query(name, status="failed", error=msg, failed_at=_now_iso())
                if self.restarts[name] < self.args.max_restarts and not self.args.available_now:
                    self.restarts[name] += 1
                    delay = restart_delay_s(self.restarts[name])
                    log.warning(f"restarting {name} from its checkpoint in {delay} s "
                                f"(attempt {self.restarts[name]}/{self.args.max_restarts})")
                    t_wait = time.time()
                    while time.time() - t_wait < delay and not self.stop_requested:
                        time.sleep(1)       # interruptible, so a stop request is honoured promptly
                    if self.stop_requested:
                        break
                    self.last_restart[name] = time.time()
                    self.start_query(name)
                else:
                    exit_code = 1
                    self.queries.pop(name)
            if not active and all(not q.isActive for q in self.queries.values()):
                if self.args.available_now or not self.queries:
                    break
            time.sleep(2)
        self.shutdown()
        self.board.update(state="stopped", stopped_at=_now_iso(), exit_code=exit_code)
        return exit_code

    def shutdown(self, wait_s: float = 45.0) -> None:
        """Stop all queries concurrently. Each waits until its in-flight micro-batch has
        completed (its batch id advances) - batches can take longer than the trigger
        interval, so "trigger not active" may never be observed - then stops. The total
        stays inside the container's 90 s stop grace period."""
        def stop_one(name: str, q: StreamingQuery) -> None:
            try:
                if not q.isActive:
                    return
                t0 = time.time()
                first = (q.lastProgress or {}).get("batchId")
                while q.status.get("isTriggerActive") and time.time() - t0 < wait_s:
                    if (q.lastProgress or {}).get("batchId") != first:
                        break                    # the batch that was running has committed
                    time.sleep(0.5)
                q.stop()
                log.info(f"stopped query {name} after {time.time() - t0:.1f} s")
            except Exception as exc:  # noqa: BLE001 - keep stopping the others
                log.error(f"error while stopping query {name}: {exc}")

        threads = [threading.Thread(target=stop_one, args=(n, q), name=f"stop-{n}") for n, q in self.queries.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=wait_s + 20)


def main() -> None:
    ap = argparse.ArgumentParser(description="AEGIS Structured Streaming pipeline")
    ap.add_argument("--available-now", action="store_true", help="process all available Kafka data, then stop")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = until stopped)")
    ap.add_argument("--starting-offsets", choices=["earliest", "latest"], default=None)
    ap.add_argument("--queries", nargs="*", default=list(QUERIES), choices=list(QUERIES))
    ap.add_argument("--no-hbase", action="store_true", help="disable HBase sinks (Parquet + Kafka only)")
    ap.add_argument("--require-hbase", action="store_true", help="exit if HBase is unreachable at startup")
    ap.add_argument("--reset", action="store_true", help="delete checkpoints before starting")
    ap.add_argument("--max-restarts", type=int, default=5)
    args = ap.parse_args()
    cfg = get_config()
    s = cfg["streaming"]
    spark = build_spark("streaming", cfg, master=s.get("spark_master"), driver_memory=s.get("driver_memory"),
                        shuffle_partitions=4)
    pipe = Pipeline(spark, cfg, args)
    signal.signal(signal.SIGINT, pipe.request_stop)
    signal.signal(signal.SIGTERM, pipe.request_stop)
    code = 1
    try:
        code = pipe.run()
    except Exception as exc:  # noqa: BLE001 - record the crash so monitoring does not show "running"
        log.error(f"streaming driver crashed: {type(exc).__name__}: {str(exc).splitlines()[0][:400]}")
        pipe.board.update(state="crashed", stopped_at=_now_iso(), exit_code=1,
                          error=f"{type(exc).__name__}: {str(exc).splitlines()[0][:400]}")
        raise
    finally:
        try:
            spark.stop()
        except Exception:  # noqa: BLE001 - JVM may already be gone after a crash
            pass
    raise SystemExit(code)


if __name__ == "__main__":
    main()
