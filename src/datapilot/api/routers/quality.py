"""Data quality reporting endpoints."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, status

from datapilot.dependencies.container import PipelineService, get_pipeline_service
from datapilot.schemas.domain import QualityReport

router = APIRouter(prefix="/api/v1/quality", tags=["Data Quality"])
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@router.get("/report", response_model=QualityReport)
def get_quality_report(
    run_id: str | None = None,
    service: PipelineService = Depends(get_pipeline_service),
) -> QualityReport:
    """Retrieve the data quality report for run_id or the latest evaluated run."""
    if run_id is not None:
        cleaned = run_id.strip()
        if not cleaned or not _SAFE_ID_RE.match(cleaned):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"run_id contains invalid characters: {run_id!r}",
            )
        run_id = cleaned

    report = service.get_quality_report(run_id=run_id)
    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quality report not found for run_id={run_id or 'latest'}",
        )
    return report
