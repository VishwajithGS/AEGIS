"""Centralised configuration.

Values come from ``config/aegis.yaml`` (or the file named by ``AEGIS_CONFIG``)
and can be overridden by environment variables of the form
``AEGIS__<SECTION>__<KEY>``. Values from the environment are parsed as YAML
scalars, so ``true``, ``9090`` and ``0.5`` keep their types.
"""
from __future__ import annotations

import copy
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "aegis.yaml"
ENV_PREFIX = "AEGIS__"


class ConfigError(RuntimeError):
    """Raised when the configuration file is missing or invalid."""


def _apply_env_overrides(cfg: dict, environ: Mapping[str, str]) -> dict:
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        path = [p.lower() for p in name[len(ENV_PREFIX):].split("__") if p]
        if not path:
            continue
        node = cfg
        for key in path[:-1]:
            child = node.get(key)
            if not isinstance(child, dict):
                child = {}
                node[key] = child
            node = child
        try:
            value = yaml.safe_load(raw)
        except yaml.YAMLError:
            value = raw
        node[path[-1]] = value
    return cfg


def _validate(cfg: dict) -> None:
    required = ["region", "kafka", "spark", "streaming", "storage", "hbase", "risk", "generator", "model"]
    missing = [k for k in required if k not in cfg]
    if missing:
        raise ConfigError(f"configuration is missing sections: {missing}")
    weights = [f["weight"] for f in cfg["risk"]["features"].values()]
    if any(w < 0 for w in weights) or sum(weights) <= 0:
        raise ConfigError("risk feature weights must be non-negative with a positive sum")
    for name, feat in cfg["risk"]["features"].items():
        if feat["high"] <= feat["low"]:
            raise ConfigError(f"risk feature '{name}': 'high' must be greater than 'low'")
    region = cfg["region"]
    if not (region["min_lat"] < region["max_lat"] and region["min_lon"] < region["max_lon"]):
        raise ConfigError("region bounding box is inverted")


def load_config(path: str | os.PathLike | None = None,
                environ: Mapping[str, str] | None = None) -> dict:
    """Load configuration with environment overrides applied (uncached)."""
    environ = os.environ if environ is None else environ
    cfg_path = Path(path or environ.get("AEGIS_CONFIG") or DEFAULT_CONFIG_PATH)
    if not cfg_path.is_file():
        raise ConfigError(f"configuration file not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    cfg = _apply_env_overrides(copy.deepcopy(cfg), environ)
    _validate(cfg)
    return cfg


@lru_cache(maxsize=1)
def get_config() -> dict:
    """Process-wide cached configuration."""
    return load_config()


def data_root(cfg: dict) -> Path:
    root = Path(cfg["storage"]["data_root"])
    return root if root.is_absolute() else PROJECT_ROOT / root


def data_path(cfg: dict, key: str, *parts: str) -> Path:
    """Resolve a storage directory such as ``lake_dir`` plus optional sub-paths."""
    base = data_root(cfg) / cfg["storage"][key]
    return base.joinpath(*parts) if parts else base


def topic(cfg: dict, name: str) -> str:
    return cfg["kafka"]["topics"][name]


def source_topics(cfg: dict) -> list[str]:
    t = cfg["kafka"]["topics"]
    return [t["weather"], t["water_level"], t["traffic"], t["incidents"]]


def redact(value: Any) -> Any:
    """Mask anything that looks like a credential before it is logged."""
    if isinstance(value, dict):
        return {k: ("***" if any(s in k.lower() for s in ("password", "secret", "token", "key"))
                    else redact(v)) for k, v in value.items()}
    return value
