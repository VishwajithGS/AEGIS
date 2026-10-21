"""Structured logging shared by every AEGIS process.

Logs are single-line ``key=value`` records so they stay readable in a
terminal and can still be grepped by component.
"""
from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False


class _ComponentFilter(logging.Filter):
    def __init__(self, component: str):
        super().__init__()
        self.component = component

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "component"):
            # aegis.<component> loggers carry their own component; others use the process default.
            record.component = record.name[6:] if record.name.startswith("aegis.") else self.component
        return True


def setup_logging(component: str, level: str | None = None) -> logging.Logger:
    """Configure root logging once and return the component logger."""
    global _CONFIGURED
    level_name = (level or os.environ.get("AEGIS_LOG_LEVEL") or "INFO").upper()
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s level=%(levelname)s component=%(component)s logger=%(name)s msg=\"%(message)s\"",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))
        handler.addFilter(_ComponentFilter(component))
        root = logging.getLogger()
        root.handlers[:] = [handler]
        root.setLevel(level_name)
        # Third-party libraries are noisy at INFO.
        for noisy in ("py4j", "urllib3", "httpx", "thriftpy2", "happybase", "uvicorn.access"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _CONFIGURED = True
    return logging.getLogger(f"aegis.{component}")
