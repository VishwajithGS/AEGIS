"""Background Kafka consumer for live dashboard updates.

Consumes ``zone-risk-updates`` and ``data-quality-events`` (published by the
streaming job) with a per-process consumer group starting at the latest
offset, keeps a bounded in-memory event log for Server-Sent Events, and
raises dashboard alerts when a zone update falls in a configured category.
Alerts are internal to the AEGIS dashboard; nothing is sent to any external
or emergency service.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from aegis.logging_config import setup_logging

log = setup_logging("api.live-feed")


class LiveFeed:
    def __init__(self, cfg: dict, enabled: bool = True, maxlen: int = 500):
        self.cfg, self.enabled = cfg, enabled
        self.events: deque = deque(maxlen=maxlen)
        self.alerts: deque = deque(maxlen=int(cfg["alerts"]["max_recent"]))
        self._alert_keys: set = set()
        self.seq = 0
        self.lock = threading.Lock()
        self.status = {"state": "not_started", "connected": False, "last_message_at": None, "error": None,
                       "messages": 0}
        self._stop = threading.Event()
        self.alert_categories = set(cfg["alerts"]["categories"])

    def start(self) -> None:
        if not self.enabled:
            self.status["state"] = "disabled"
            return
        threading.Thread(target=self._run, name="live-feed", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _push(self, kind: str, data: dict) -> None:
        with self.lock:
            self.seq += 1
            self.events.append({"seq": self.seq, "type": kind, "data": data,
                                "received_at": datetime.now(timezone.utc).isoformat()})

    def _maybe_alert(self, upd: dict) -> None:
        cat = upd.get("risk_category")
        if cat not in self.alert_categories:
            return
        key = (upd.get("zone_id"), str(upd.get("window_start")), cat)
        if key in self._alert_keys:
            return
        self._alert_keys.add(key)
        if len(self._alert_keys) > 5000:
            self._alert_keys.clear()
        alert = {"alert_id": uuid.uuid4().hex[:12], "zone_id": upd.get("zone_id"), "risk_category": cat,
                 "risk_score": upd.get("risk_score"), "confidence": upd.get("confidence"),
                 "window_start": upd.get("window_start"), "window_end": upd.get("window_end"),
                 "explanation": upd.get("explanation"), "raised_at": datetime.now(timezone.utc).isoformat(),
                 "rule": f"zone-risk update with category in {sorted(self.alert_categories)}",
                 "source": "AEGIS rule-based zone risk (Kafka topic zone-risk-updates)",
                 "delivery": "dashboard only - not sent to any external or emergency service"}
        with self.lock:
            self.alerts.appendleft(alert)
        self._push("alert", alert)

    def _run(self) -> None:
        from confluent_kafka import Consumer, KafkaError
        t = self.cfg["kafka"]["topics"]
        topics = [t["zone_risk"], t["data_quality"]]
        while not self._stop.is_set():
            consumer = None
            try:
                consumer = Consumer({
                    "bootstrap.servers": self.cfg["kafka"]["bootstrap_servers"],
                    "group.id": f"{self.cfg['kafka']['api_consumer_group']}-{uuid.uuid4().hex[:8]}",
                    "auto.offset.reset": "latest", "enable.auto.commit": False,
                    "socket.timeout.ms": 10000, "session.timeout.ms": 15000,
                })
                consumer.list_topics(timeout=10)
                consumer.subscribe(topics)
                self.status.update(state="running", connected=True, error=None)
                log.info(f"live feed subscribed to {topics}")
                while not self._stop.is_set():
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            self.status["error"] = str(msg.error())
                        continue
                    try:
                        data = json.loads(msg.value())
                    except (ValueError, TypeError):
                        continue
                    self.status["messages"] += 1
                    self.status["last_message_at"] = datetime.now(timezone.utc).isoformat()
                    if msg.topic() == t["zone_risk"]:
                        self._push("zone_risk", data)
                        self._maybe_alert(data)
                    else:
                        self._push("data_quality", {k: data.get(k) for k in ("batch_id", "processed_at", "records",
                                                                             "valid", "rejected")})
            except Exception as exc:  # noqa: BLE001 - keep retrying; report the error in status
                self.status.update(state="retrying", connected=False, error=f"{type(exc).__name__}: {exc}")
                log.warning(f"live feed error (retry in 10 s): {exc}")
                self._stop.wait(10)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:  # noqa: BLE001
                        pass

    def since(self, seq: int, limit: int = 200) -> list[dict]:
        with self.lock:
            return [e for e in self.events if e["seq"] > seq][-limit:]

    def recent_alerts(self, limit: int = 50) -> list[dict]:
        with self.lock:
            return list(self.alerts)[:limit]
