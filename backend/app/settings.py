"""API settings layered on the shared AEGIS configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache

from aegis.config import get_config


@dataclass
class ApiSettings:
    cfg: dict
    cors_origins: list = field(default_factory=list)
    cache_ttl_s: float = 5.0
    parquet_ttl_s: float = 15.0
    live_stale_after_s: float = 900.0
    hbase_timeout_ms: int = 4000
    enable_live_feed: bool = True
    enable_model: bool = True


@lru_cache(maxsize=1)
def get_settings() -> ApiSettings:
    origins = os.environ.get("AEGIS_API_CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
    return ApiSettings(
        cfg=get_config(),
        cors_origins=[o.strip() for o in origins.split(",") if o.strip()],
        enable_live_feed=os.environ.get("AEGIS_API_LIVE_FEED", "1") == "1",
        enable_model=os.environ.get("AEGIS_API_MODEL", "1") == "1",
    )
