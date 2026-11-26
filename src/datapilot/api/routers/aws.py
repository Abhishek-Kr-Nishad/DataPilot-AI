"""AWS configuration status, Athena SQL assets, and explicit S3 sync endpoints."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, status

from datapilot.dependencies.container import AWSService, get_aws_service
from datapilot.schemas.api import AWSSyncResponse, AthenaSQLResponse
from datapilot.storage.s3_storage import AWSIntegrationError

router = APIRouter(prefix="/api/v1/aws", tags=["AWS & Athena"])
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@router.get("/status")
def get_aws_status(service: AWSService = Depends(get_aws_service)) -> dict[str, object]:
    """Return credential-safe AWS readiness and execution mode separation details."""
    return service.get_status()


@router.get("/athena-sql", response_model=AthenaSQLResponse)
def get_athena_sql(service: AWSService = Depends(get_aws_service)) -> AthenaSQLResponse:
    """Return Amazon Athena DDL and validation SQL scripts."""
    return service.get_athena_sql()


@router.post("/sync/{run_id}", response_model=AWSSyncResponse)
def sync_run_to_aws(
    run_id: str,
    service: AWSService = Depends(get_aws_service),
) -> AWSSyncResponse:
    """Upload materialized local run artifacts to S3 when AWS credentials are configured."""
    cleaned = run_id.strip()
    if not cleaned or not _SAFE_ID_RE.match(cleaned):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"run_id contains invalid characters: {run_id!r}",
        )
    try:
        return service.sync_run(cleaned)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except AWSIntegrationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
