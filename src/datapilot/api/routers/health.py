"""Health and runtime mode inspection endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from datapilot import __version__
from datapilot.dependencies.container import ServiceContainer, get_container
from datapilot.schemas.api import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
def health_check(container: ServiceContainer = Depends(get_container)) -> HealthResponse:
    """Return service health, active execution mode, DB dialect, and sanitized AWS readiness."""
    cfg = container.settings
    db_backend = (
        container.db_manager.database_url.split(":", 1)[0]
        if container.db_manager is not None
        else "json"
    )
    return HealthResponse(
        status="ok",
        service="datapilot-ai",
        version=__version__,
        requested_mode=cfg.requested_mode,
        effective_mode=cfg.effective_mode,
        fallback_reason=cfg.fallback_reason,
        database_backend=db_backend,
        gemini_configured=cfg.has_gemini_key,
        auth_enabled=cfg.auth_enabled,
        aws=cfg.aws.sanitized_summary(),
    )
