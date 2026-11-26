"""Single FastAPI application factory and ASGI entrypoint for DataPilot AI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
import uuid

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession

from datapilot import __version__
from datapilot.api.routers import agent, analytics, aws, health, pipeline, quality
from datapilot.api.security import require_api_auth
from datapilot.core.config import AppSettings, load_settings
from datapilot.core.logging import get_logger
from datapilot.dependencies.container import build_service_container

logger = get_logger("datapilot.api")


def create_app(
    settings: AppSettings | None = None,
    spark: SparkSession | None = None,
    gemini_client: Any | None = None,
) -> FastAPI:
    """Create and configure the single DataPilot AI FastAPI application."""
    preloaded_settings = settings
    resolved_for_app = preloaded_settings if preloaded_settings is not None else load_settings()

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI):
        if getattr(fastapi_app.state, "container", None) is None:
            resolved = preloaded_settings or load_settings()
            fastapi_app.state.container = build_service_container(
                resolved,
                spark=spark,
                gemini_client=gemini_client,
            )
        active_settings = fastapi_app.state.container.settings
        if not active_settings.auth_enabled:
            logger.warning(
                "API authentication is disabled (local dev mode). Set DATAPILOT_API_KEY to enforce X-API-Key/Bearer auth."
            )
        yield

    fastapi_app = FastAPI(
        title="DataPilot AI – Agentic Data Engineering & Analytics API",
        description=(
            "Modular FastAPI control plane for PySpark Medallion ETL (Bronze/Silver/Gold), "
            "data quality gates, Google Gemini diagnostic tool-calling with human approval, "
            "and AWS S3 / Glue / Athena integration."
        ),
        version=__version__,
        lifespan=lifespan,
    )

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=resolved_for_app.cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-API-Key"],
    )

    if preloaded_settings is not None:
        fastapi_app.state.container = build_service_container(
            preloaded_settings,
            spark=spark,
            gemini_client=gemini_client,
        )

    protected = [Depends(require_api_auth)]
    fastapi_app.include_router(health.router)
    fastapi_app.include_router(pipeline.router, dependencies=protected)
    fastapi_app.include_router(quality.router, dependencies=protected)
    fastapi_app.include_router(analytics.router, dependencies=protected)
    fastapi_app.include_router(agent.router, dependencies=protected)
    fastapi_app.include_router(aws.router, dependencies=protected)

    @fastapi_app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        error_ref = uuid.uuid4().hex[:12]
        logger.exception(
            "Unhandled exception [%s] on %s %s", error_ref, request.method, request.url.path
        )
        if resolved_for_app.debug:
            detail = f"{type(exc).__name__}: {exc} (reference: {error_ref})"
        else:
            detail = f"Internal server error (reference: {error_ref})"
        return JSONResponse(
            status_code=500,
            content={"detail": detail},
        )

    return fastapi_app


app = create_app()
