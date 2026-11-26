"""Gold layer business and operational analytics endpoints."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, status

from datapilot.dependencies.container import AnalyticsService, get_analytics_service
from datapilot.schemas.api import AnalyticsSummaryResponse

router = APIRouter(prefix="/api/v1/analytics", tags=["Analytics"])
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@router.get("/summary", response_model=AnalyticsSummaryResponse)
def get_analytics_summary(
    run_id: str | None = None,
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsSummaryResponse:
    """Return aggregated Gold revenue, category, regional, and quality KPIs."""
    if run_id is not None:
        cleaned = run_id.strip()
        if not cleaned or not _SAFE_ID_RE.match(cleaned):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"run_id contains invalid characters: {run_id!r}",
            )
        run_id = cleaned
    return service.get_summary(run_id=run_id)
