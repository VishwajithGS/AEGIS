"""HBase queries used by the API (short timeouts, no retries, so a missing
HBase degrades the response instead of hanging it)."""
from __future__ import annotations

import json
import time
from typing import Optional

from aegis.storage.hbase_repository import HBaseRepository, HBaseUnavailable


class HBaseService:
    def __init__(self, cfg: dict, timeout_ms: int = 4000):
        h = cfg["hbase"]
        self.enabled = bool(h.get("enabled", True))
        self.host, self.port, self.timeout_ms = h["host"], int(h["port"]), timeout_ms
        self._down_until = 0.0     # brief circuit breaker after a failure

    def _repo(self) -> HBaseRepository:
        if not self.enabled:
            raise HBaseUnavailable("HBase disabled in configuration")
        if time.time() < self._down_until:
            raise HBaseUnavailable("HBase recently unreachable (retrying in a few seconds)")
        return HBaseRepository(self.host, self.port, self.timeout_ms, write_retries=1, retry_backoff_s=0)

    def _fail(self) -> None:
        self._down_until = time.time() + 10

    def health(self) -> dict:
        if not self.enabled:
            return {"available": False, "error": "disabled in configuration"}
        repo = HBaseRepository(self.host, self.port, self.timeout_ms, write_retries=1)
        try:
            h = repo.health()
            if not h["available"]:
                self._fail()
            else:
                self._down_until = 0.0
            return h
        finally:
            repo.close()

    @staticmethod
    def _risk_row(key: str, fams: dict) -> dict:
        risk, summary, quality = fams.get("risk", {}), fams.get("summary", {}), fams.get("quality", {})
        contrib = risk.get("contributions")
        if isinstance(contrib, str):
            try:
                contrib = json.loads(contrib)
            except ValueError:
                pass
        zone_id = key.split("#", 1)[0]
        return {"zone_id": zone_id, "row_key": key, **risk, "contributions": contrib, **summary, **quality}

    def latest_zone_risk(self, zone_ids: list[str]) -> dict[str, dict]:
        repo = self._repo()
        try:
            out = {}
            for z in zone_ids:
                row = repo.latest_with_prefix("zone_risk_history", f"{z}#live#")
                if row:
                    out[z] = self._risk_row(*row)
            return out
        except HBaseUnavailable:
            self._fail()
            raise
        finally:
            repo.close()

    def zone_history(self, zone_id: str, limit: int = 48) -> list[dict]:
        repo = self._repo()
        try:
            return [self._risk_row(k, f) for k, f in
                    repo.scan("zone_risk_history", row_prefix=f"{zone_id}#live#", limit=limit, reverse=True)]
        except HBaseUnavailable:
            self._fail()
            raise
        finally:
            repo.close()

    def score_versions(self, row_key: str, versions: int = 5) -> list[dict]:
        repo = self._repo()
        try:
            return repo.cell_versions("zone_risk_history", row_key, "risk:risk_score", versions)
        finally:
            repo.close()

    def incident(self, incident_id: str) -> Optional[dict]:
        repo = self._repo()
        try:
            return repo.get("incident_records", incident_id)
        except HBaseUnavailable:
            self._fail()
            raise
        finally:
            repo.close()

    def latest_metrics(self, components: list[str]) -> dict:
        repo = self._repo()
        try:
            out = {}
            for c in components:
                row = repo.latest_with_prefix("processing_metrics", f"{c}#")
                if row:
                    out[c] = {"row_key": row[0], **row[1]}
            return out
        finally:
            repo.close()

    def put_prediction(self, row_key: str, families: dict) -> bool:
        repo = self._repo()
        try:
            repo.put("model_predictions", row_key, families)
            return repo.get("model_predictions", row_key) is not None    # read-back verification
        finally:
            repo.close()

    def recent_predictions(self, subject: str = "api", limit: int = 20) -> list[dict]:
        repo = self._repo()
        try:
            return [{"row_key": k, **f} for k, f in repo.scan("model_predictions", row_prefix=f"{subject}#", limit=limit)]
        finally:
            repo.close()
