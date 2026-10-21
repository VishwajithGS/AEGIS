"""Live HBase read/write verification (probe rows are deleted afterwards)."""
from __future__ import annotations

import time
import uuid

import pytest

from aegis.storage.hbase_repository import (HBaseRepository, HBaseUnavailable, sensor_row_key, zone_risk_row_key)

pytestmark = pytest.mark.integration


def test_put_get_scan_versions_delete(require_hbase):
    repo = HBaseRepository.from_config(require_hbase)
    tag = uuid.uuid4().hex[:8]
    try:
        assert set(repo.ensure_tables().values()) <= {"created", "exists"}
        now = int(time.time() * 1000)
        rows = [(sensor_row_key(f"IT-{tag}", now + i, f"ev-{i}"),
                 {"measurement": {"rainfall_mm_hr": float(i)}, "metadata": {"event_id": f"ev-{i}"}}) for i in range(50)]
        assert repo.put_many("sensor_observations", rows) == 50
        got = repo.get("sensor_observations", rows[7][0])
        assert got == {"measurement": {"rainfall_mm_hr": 7.0}, "metadata": {"event_id": "ev-7"}}
        assert repo.count_rows("sensor_observations", row_prefix=f"IT-{tag}#") == 50
        newest = repo.latest_with_prefix("sensor_observations", f"IT-{tag}#")
        assert newest[0] == rows[-1][0]                              # reverse scan -> latest timestamp first
        key = zone_risk_row_key(f"IT-{tag}", now)
        for score in (10.0, 20.0, 30.0):
            repo.put("zone_risk_history", key, {"risk": {"risk_score": score}})
        versions = [v["value"] for v in repo.cell_versions("zone_risk_history", key, "risk:risk_score", 5)]
        assert versions == [30.0, 20.0, 10.0]
        for k, _ in rows:
            repo.delete("sensor_observations", k)
        repo.delete("zone_risk_history", key)
        assert repo.count_rows("sensor_observations", row_prefix=f"IT-{tag}#") == 0
    finally:
        repo.close()


def test_unreachable_hbase_raises_clear_error():
    repo = HBaseRepository("127.0.0.1", 1, timeout_ms=1000, write_retries=1)
    with pytest.raises(HBaseUnavailable):
        repo.connect()
    assert repo.health()["available"] is False
