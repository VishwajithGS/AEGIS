"""Re-publish records from the producer dead-letter file after an outage.

    python -m producers.replay_dead_letter            # replays and archives the file
"""
from __future__ import annotations

import json
import time

from aegis.config import data_path, get_config
from aegis.logging_config import setup_logging
from producers.kafka_client import AegisProducer

log = setup_logging("dead-letter-replay")


def main() -> None:
    cfg = get_config()
    path = data_path(cfg, "dead_letter_dir", "producer_failures.jsonl")
    if not path.exists():
        print(json.dumps({"replayed": 0, "note": "no dead-letter file"}))
        return
    archived = path.with_name(f"producer_failures.replayed-{int(time.time())}.jsonl")
    path.rename(archived)          # new failures during replay go to a fresh file
    producer = AegisProducer(cfg, client_id="aegis-dead-letter-replay")
    n = 0
    with archived.open(encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if rec.get("value") is None:
                continue
            producer.send(rec["topic"], rec.get("key") or "replay", rec["value"])
            n += 1
    remaining = producer.flush(60)
    out = {"replayed": n, "delivered": producer.stats["delivered"], "failed": producer.stats["failed"],
           "undelivered": remaining, "archived_file": str(archived)}
    log.info(json.dumps(out))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
