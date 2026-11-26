"""Single FastAPI application factory and ASGI entrypoint for DataPilot AI."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession

from datapilot import __version__
from datapilot.api.routers import agent, analytics, aws, health, pipeline, quality
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

    @asynccontextmanager
    async def lifespan(fastapi_app: FastAPI):
        if getattr(fastapi_app.state, "container", None) is None:
            resolved = preloaded_settings or load_settings()
            fastapi_app.state.container = build_service_container(
                resolved,
                spark=spark,
                gemini_client=gemini_client,
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

    cors_origins = (
        preloaded_settings.cors_origins
        if preloaded_settings is not None
        else ["http://localhost:3000", "http://localhost:5173"]
    )
    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if preloaded_settings is not None:
        fastapi_app.state.container = build_service_container(
            preloaded_settings,
            spark=spark,
            gemini_client=gemini_client,
        )

    fastapi_app.include_router(health.router)
    fastapi_app.include_router(pipeline.router)
    fastapi_app.include_router(quality.router)
    fastapi_app.include_router(analytics.router)
    fastapi_app.include_router(agent.router)
    fastapi_app.include_router(aws.router)

    @fastapi_app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": f"{type(exc).__name__}: {exc}"},
        )

    return fastapi_app


app = create_app()
