from pathlib import Path

import pytest
import yaml

from aegis.config import ConfigError, data_path, load_config, redact, source_topics


def test_env_overrides_are_typed():
    cfg = load_config(environ={"AEGIS__KAFKA__BOOTSTRAP_SERVERS": "kafka:29092", "AEGIS__HBASE__PORT": "9091",
                               "AEGIS__HBASE__ENABLED": "false", "AEGIS__RISK__FEATURES__RAINFALL__WEIGHT": "0.5"})
    assert cfg["kafka"]["bootstrap_servers"] == "kafka:29092"
    assert cfg["hbase"]["port"] == 9091 and cfg["hbase"]["enabled"] is False
    assert cfg["risk"]["features"]["rainfall"]["weight"] == 0.5


def test_topics_and_paths(cfg, tmp_path):
    assert source_topics(cfg) == ["weather-events", "water-level-events", "traffic-events", "incident-reports"]
    c = load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(tmp_path)})
    assert data_path(c, "lake_dir", "silver") == tmp_path / "lake" / "silver"


def test_invalid_risk_config_rejected(tmp_path):
    src = yaml.safe_load(Path("config/aegis.yaml").read_text(encoding="utf-8"))
    src["risk"]["features"]["rainfall"]["high"] = 0.0      # high <= low
    p = tmp_path / "bad.yaml"
    p.write_text(yaml.safe_dump(src), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(p, environ={})


def test_missing_config_file(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.yaml", environ={})


def test_redact_masks_secret_like_keys():
    assert redact({"password": "x", "nested": {"api_token": "y", "host": "h"}}) == \
        {"password": "***", "nested": {"api_token": "***", "host": "h"}}
