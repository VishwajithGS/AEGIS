"""Shared live-publishing loop used by every source producer."""
from __future__ import annotations

import argparse
import json
import signal
import time
from typing import Iterable, Optional

from aegis.config import data_path, get_config, topic
from aegis.logging_config import setup_logging
from producers.data_generator import TOPIC_KEYS, LiveGenerator
from producers.kafka_client import AegisProducer

STATS_EVERY_S = 15.0


def _write_status(path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def basic_envelope_ok(payload: str, max_bytes: int = 1_000_000) -> bool:
    """Pre-publish sanity check: the payload must be text below the size limit.

    Deliberately-injected defective payloads still pass this check because the
    pipeline's validation stage - not the producer - must catch them."""
    return isinstance(payload, str) and 0 < len(payload.encode("utf-8")) <= max_bytes


def run(sources: Iterable[str], description: str, argv: Optional[list[str]] = None) -> dict:
    cfg = get_config()
    g = cfg["generator"]
    ap = argparse.ArgumentParser(description=description + " (SYNTHETIC data)")
    ap.add_argument("--rate", type=float, default=g["live_rate_per_sec"], help="events per second (all sources in this process)")
    ap.add_argument("--duration", type=float, default=g["live_duration_sec"], help="seconds to run; 0 = until Ctrl+C")
    ap.add_argument("--max-events", type=int, default=0)
    ap.add_argument("--seed", type=int, default=g["seed"])
    ap.add_argument("--anomaly-rate", type=float, default=g["anomaly_rate"])
    ap.add_argument("--late-rate", type=float, default=0.03)
    ap.add_argument("--bootstrap", default=None, help="override kafka.bootstrap_servers")
    args = ap.parse_args(argv)

    sources = set(sources)
    name = "+".join(sorted(sources)) if len(sources) < len(TOPIC_KEYS) else "all-sources"
    log = setup_logging(f"producer.{name}")
    producer = AegisProducer(cfg, args.bootstrap, client_id=f"aegis-{name}")
    try:
        topics_on_broker = producer.check_connection(timeout=15)
    except Exception as exc:  # noqa: BLE001 - report any connectivity failure clearly
        log.error(f"cannot reach Kafka at {producer.conf['bootstrap.servers']}: {exc}")
        raise SystemExit(2)
    missing = [topic(cfg, s) for s in sources if topic(cfg, s) not in topics_on_broker]
    if missing:
        log.error(f"topics missing on broker: {missing}; run `python -m producers.kafka_admin create` first")
        raise SystemExit(2)

    gen = LiveGenerator(cfg, seed=args.seed, rate_per_sec=args.rate, sources=sources,
                        anomaly_rate=args.anomaly_rate, late_rate=args.late_rate)
    stop = {"flag": False}

    def _stop(*_):
        stop["flag"] = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info(f"publishing {sorted(sources)} at ~{args.rate}/s anomaly_rate={args.anomaly_rate} "
             f"duration={args.duration or 'infinite'} accel={gen.accel:.1f}x")
    last = time.time()
    started = last
    status_path = data_path(cfg, "state_dir", f"producer_{name}_status.json")
    rejected_local = 0
    for e in gen.events(duration_sec=args.duration, max_events=args.max_events,
                        should_stop=lambda: stop["flag"]):
        if stop["flag"]:
            break
        if not basic_envelope_ok(e.value):
            rejected_local += 1
            continue
        producer.send(topic(cfg, e.topic_key), e.key, e.value)
        if time.time() - last >= STATS_EVERY_S:
            last = time.time()
            log.info(f"stats sent={producer.stats['sent']} delivered={producer.stats['delivered']} "
                     f"failed={producer.stats['failed']} kinds={dict(gen.counts)}")
            _write_status(status_path, {"running": True, "sources": sorted(sources), "rate_per_sec": args.rate,
                                        "updated_at": time.time(), "started_at": started,
                                        "producer": dict(producer.stats), "generator": dict(gen.counts)})
    remaining = producer.flush(30)
    summary = {
        "sources": sorted(sources), "sent": producer.stats["sent"], "delivered": producer.stats["delivered"],
        "failed": producer.stats["failed"], "undelivered_after_flush": remaining,
        "rejected_before_publish": rejected_local, "generator_counts": dict(sorted(gen.counts.items())),
        "synthetic": True,
    }
    _write_status(status_path, {"running": False, "updated_at": time.time(), "started_at": started, **summary})
    log.info(f"producer finished: {json.dumps(summary)}")
    return summary
