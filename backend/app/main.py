"""AEGIS FastAPI application.

    uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
OpenAPI / Swagger UI: http://localhost:8000/docs
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from aegis.config import redact
from aegis.logging_config import setup_logging
from backend.app.api.deps import build_services, set_services
from backend.app.api.routes import router
from backend.app.settings import get_settings

log = setup_logging("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    svc = build_services(settings)
    set_services(svc)
    log.info(f"API starting; kafka={settings.cfg['kafka']['bootstrap_servers']} "
             f"hbase={settings.cfg['hbase']['host']}:{settings.cfg['hbase']['port']} "
             f"cors={redact(settings.cors_origins)}")
    svc.feed.start()
    svc.model.start()
    yield
    svc.feed.stop()
    log.info("API stopped")


app = FastAPI(
    title="AEGIS API",
    version="1.0.0",
    description=("Real-time disaster intelligence over SYNTHETIC demo data: rule-based zone risk, incidents, "
                 "historical analytics, pipeline status and a supporting Spark MLlib severity classifier."),
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=get_settings().cors_origins, allow_methods=["GET", "POST"],
                   allow_headers=["*"])
app.include_router(router)


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log.error(f"unhandled error on {request.method} {request.url.path}: {type(exc).__name__}: {exc}")
    return JSONResponse(status_code=500, content={"detail": f"internal error: {type(exc).__name__}"})
