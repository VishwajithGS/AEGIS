import pytest

from aegis.storage.hbase_repository import (MAX_TS_MS, TABLES, from_cells, metric_row_key, prediction_row_key,
                                            sensor_row_key, to_cells, zone_risk_row_key)


def test_row_key_formats():
    assert sensor_row_key("WXM-01", 1760935500000, "WX-abc") == "WXM-01#1760935500000#WX-abc"
    assert sensor_row_key("WXM-01", 5, "e") == "WXM-01#0000000000005#e"      # fixed width keeps lexical = time order
    assert zone_risk_row_key("Z-R01-C02", 1760935500000) == "Z-R01-C02#live#1760935500000"
    assert zone_risk_row_key("Z-R01-C02", 1760935500000, "hourly") == "Z-R01-C02#hourly#1760935500000"
    with pytest.raises(ValueError):
        zone_risk_row_key("Z", 1, "weekly")
    # same instant, different series -> different rows (no cross-overwrite)
    assert zone_risk_row_key("Z", 1, "live") != zone_risk_row_key("Z", 1, "hourly")
    assert metric_row_key("stream.silver", 42) == "stream.silver#0000000000042"
    with pytest.raises(ValueError):
        sensor_row_key("s", -1, "e")


def test_time_ordering_of_keys():
    keys = [zone_risk_row_key("Z", t) for t in (9, 100, 1760935500000)]
    assert keys == sorted(keys)
    # reverse timestamps: newest prediction sorts first in a forward scan
    newer, older = prediction_row_key("api", 2000, "a"), prediction_row_key("api", 1000, "b")
    assert newer < older
    assert prediction_row_key("api", 0, "x").split("#")[1] == str(MAX_TS_MS)


def test_cells_roundtrip_and_sparsity():
    cells = to_cells({"measurement": {"rainfall_mm_hr": 12.5, "level": None, "nan": float("nan"), "ok": True},
                      "metadata": {"event_id": "WX-1", "flags": ["late_arrival"], "count": 3}})
    assert b"measurement:level" not in cells and b"measurement:nan" not in cells     # absent, not stored
    back = from_cells(cells)
    assert back["measurement"] == {"rainfall_mm_hr": 12.5, "ok": True}
    assert back["metadata"] == {"event_id": "WX-1", "flags": ["late_arrival"], "count": 3}


def test_table_specs_cover_required_tables():
    assert {"sensor_observations", "incident_records", "zone_risk_history", "processing_metrics"} <= set(TABLES)
    assert set(TABLES["sensor_observations"]["families"]) == {"measurement", "location", "metadata"}
    assert TABLES["zone_risk_history"]["families"]["risk"]["max_versions"] == 5
