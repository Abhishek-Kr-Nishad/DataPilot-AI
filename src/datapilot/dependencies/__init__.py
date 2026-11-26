"""FastAPI dependency injection container."""

from datapilot.dependencies.container import (
    ServiceContainer,
    build_service_container,
    get_agent_service,
    get_analytics_service,
    get_aws_service,
    get_container,
    get_pipeline_service,
)

__all__ = [
    "ServiceContainer",
    "build_service_container",
    "get_agent_service",
    "get_analytics_service",
    "get_aws_service",
    "get_container",
    "get_pipeline_service",
]
