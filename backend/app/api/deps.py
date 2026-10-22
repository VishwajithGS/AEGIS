"""Service container shared by the routers."""
from __future__ import annotations

from dataclasses import dataclass

from backend.app.services.analytics_service import AnalyticsService
from backend.app.services.dataset_service import DatasetService
from backend.app.services.hbase_service import HBaseService
from backend.app.services.incident_service import IncidentService
from backend.app.services.live_feed import LiveFeed
from backend.app.services.model_service import ModelService
from backend.app.services.parquet_store import ParquetStore
from backend.app.services.pipeline_service import PipelineService
from backend.app.services.zone_service import ZoneService
from backend.app.settings import ApiSettings


@dataclass
class Services:
    settings: ApiSettings
    store: ParquetStore
    hbase: HBaseService
    zones: ZoneService
    incidents: IncidentService
    analytics: AnalyticsService
    model: ModelService
    feed: LiveFeed
    pipeline: PipelineService
    dataset: DatasetService


def build_services(settings: ApiSettings) -> Services:
    cfg = settings.cfg
    store = ParquetStore(cfg, settings.parquet_ttl_s)
    hbase = HBaseService(cfg, settings.hbase_timeout_ms)
    feed = LiveFeed(cfg, settings.enable_live_feed)
    model = ModelService(cfg, hbase, settings.enable_model)
    return Services(
        settings=settings, store=store, hbase=hbase,
        zones=ZoneService(cfg, store, hbase, settings.live_stale_after_s, settings.cache_ttl_s),
        incidents=IncidentService(store, hbase), analytics=AnalyticsService(cfg, store), model=model, feed=feed,
        pipeline=PipelineService(cfg, store, hbase, feed, model, settings.live_stale_after_s),
        dataset=DatasetService(cfg),
    )


_services: Services | None = None


def set_services(s: Services) -> None:
    global _services
    _services = s


def services() -> Services:
    if _services is None:
        raise RuntimeError("services not initialised")
    return _services
