"""Shared fixtures. Spark fixtures skip (with the reason) when PySpark or a
compatible Java runtime is not available in the current environment."""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

from aegis.config import load_config

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def cfg() -> dict:
    return load_config(environ={})


@pytest.fixture()
def tmp_cfg(tmp_path) -> dict:
    """Config whose storage root is an isolated temporary directory."""
    return load_config(environ={"AEGIS__STORAGE__DATA_ROOT": str(tmp_path / "data")})


def _java_ok() -> tuple[bool, str]:
    java = shutil.which("java")
    if not java:
        return False, "java not found on PATH"
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=20).stderr
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"java not runnable: {exc}"
    first = out.splitlines()[0] if out else ""
    for v in ('"17', '"11', '"21'):
        if v in first:
            return True, first
    return False, f"Spark 3.5 needs Java 11/17 (found: {first})"


@pytest.fixture(scope="session")
def spark():
    try:
        import pyspark  # noqa: F401
    except ImportError:
        pytest.skip("PySpark not installed in this environment (run tests in the aegis-spark container)")
    ok, why = _java_ok()
    if not ok:
        pytest.skip(why)
    from spark_jobs.common import build_spark
    s = build_spark("tests", load_config(environ={}), master="local[2]", shuffle_partitions=2, driver_memory="1g")
    yield s
    s.stop()


def port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def kafka_endpoint(cfg: dict) -> tuple[str, int]:
    host, port = cfg["kafka"]["bootstrap_servers"].split(",")[0].rsplit(":", 1)
    return host, int(port)


@pytest.fixture(scope="session")
def live_cfg() -> dict:
    """Configuration from the real environment (honours AEGIS__* overrides, e.g. inside Docker)."""
    return load_config(environ=dict(os.environ))


@pytest.fixture(scope="session")
def require_kafka(live_cfg):
    host, port = kafka_endpoint(live_cfg)
    if not port_open(host, port):
        pytest.skip(f"Kafka not reachable at {host}:{port}")
    return live_cfg


@pytest.fixture(scope="session")
def require_hbase(live_cfg):
    h = live_cfg["hbase"]
    if not port_open(h["host"], int(h["port"])):
        pytest.skip(f"HBase Thrift not reachable at {h['host']}:{h['port']}")
    return live_cfg
