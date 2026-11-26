"""Application-scoped dependency container and FastAPI Depends providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from pyspark.sql import SparkSession

from datapilot.agent.approval import ApprovalStore
from datapilot.core.config import AppSettings
from datapilot.database.session import DatabaseManager, create_database_manager
from datapilot.metadata.store import MetadataStore, create_metadata_store
from datapilot.services.agent_service import AgentService
from datapilot.services.analytics_service import AnalyticsService
from datapilot.services.aws_service import AWSService
from datapilot.services.pipeline_service import PipelineService


@dataclass
class ServiceContainer:
    """Holds initialized database, metadata, storage, and domain service objects."""

    settings: AppSettings
    db_manager: DatabaseManager | None
    metadata_store: MetadataStore
    approval_store: ApprovalStore
    pipeline_service: PipelineService
    analytics_service: AnalyticsService
    agent_service: AgentService
    aws_service: AWSService


def build_service_container(
    settings: AppSettings,
    spark: SparkSession | None = None,
    gemini_client: Any | None = None,
) -> ServiceContainer:
    """Wire together core settings, SQLAlchemy DB, stores, and domain services."""
    settings.ensure_directories()
    db_manager = (
        None if settings.metadata.backend == "json" else create_database_manager(settings)
    )
    metadata_store = create_metadata_store(settings, db_manager=db_manager)
    approval_store = ApprovalStore(settings.local.approvals_dir, db_manager=db_manager)

    pipeline_service = PipelineService(settings, metadata_store=metadata_store, spark=spark)
    analytics_service = AnalyticsService(settings, metadata_store=metadata_store, spark=spark)
    agent_service = AgentService(
        settings,
        metadata_store=metadata_store,
        approval_store=approval_store,
        gemini_client=gemini_client,
    )
    aws_service = AWSService(settings, metadata_store=metadata_store)

    return ServiceContainer(
        settings=settings,
        db_manager=db_manager,
        metadata_store=metadata_store,
        approval_store=approval_store,
        pipeline_service=pipeline_service,
        analytics_service=analytics_service,
        agent_service=agent_service,
        aws_service=aws_service,
    )


def get_container(request: Request) -> ServiceContainer:
    """Return the ServiceContainer attached to app.state, initializing lazily if absent."""
    container = getattr(request.app.state, "container", None)
    if container is None:
        from datapilot.core.config import load_settings

        container = build_service_container(load_settings())
        request.app.state.container = container
    return container


def get_pipeline_service(request: Request) -> PipelineService:
    """Inject PipelineService."""
    return get_container(request).pipeline_service


def get_analytics_service(request: Request) -> AnalyticsService:
    """Inject AnalyticsService."""
    return get_container(request).analytics_service


def get_agent_service(request: Request) -> AgentService:
    """Inject AgentService."""
    return get_container(request).agent_service


def get_aws_service(request: Request) -> AWSService:
    """Inject AWSService."""
    return get_container(request).aws_service
