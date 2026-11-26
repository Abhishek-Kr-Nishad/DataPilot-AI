"""Pipeline execution, status, run history, and layer preview endpoints."""

from __future__ import annotations

from pathlib import Path
import re

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.concurrency import run_in_threadpool

from datapilot.dependencies.container import (
    PipelineService,
    ServiceContainer,
    get_container,
    get_pipeline_service,
)
from datapilot.schemas.api import (
    LayerPreviewResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineStatusResponse,
)
from datapilot.schemas.domain import PipelineRunRecord

router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipeline"])
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _validate_id(value: str, label: str) -> str:
    cleaned = value.strip()
    if not cleaned or not _SAFE_ID_RE.match(cleaned):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{label} contains invalid characters: {value!r}",
        )
    return cleaned


@router.post(
    "/run",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_201_CREATED,
)
def run_synthetic_pipeline(
    payload: PipelineRunRequest,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineRunResponse:
    """Generate a reproducible synthetic batch and run the Medallion PySpark ETL pipeline."""
    try:
        return service.run_synthetic_batch(
            scenario=payload.scenario,
            num_rows=payload.num_rows,
            seed=payload.seed,
            fail_on_quality_error=payload.fail_on_quality_error,
            sync_to_aws=payload.sync_to_aws,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post(
    "/upload",
    response_model=PipelineRunResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_csv_and_run(
    file: UploadFile = File(...),
    fail_on_quality_error: bool = Form(True),
    sync_to_aws: bool = Form(False),
    container: ServiceContainer = Depends(get_container),
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineRunResponse:
    """Upload a raw CSV file and process it through Bronze -> Silver -> Quality -> Gold."""
    raw_name = Path(file.filename or "uploaded.csv").name
    if not raw_name.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are supported for raw ingestion.",
        )

    max_bytes = container.settings.max_upload_bytes
    chunks = bytearray()
    chunk_size = 1024 * 1024
    try:
        while chunk := await file.read(chunk_size):
            chunks.extend(chunk)
            if len(chunks) > max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=(
                        f"Uploaded CSV exceeds maximum allowed size ({container.settings.max_upload_mb} MB)."
                    ),
                )
    finally:
        await file.close()

    if not chunks:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded CSV file is empty.",
        )

    try:
        return await run_in_threadpool(
            service.run_uploaded_csv,
            filename=raw_name,
            content=bytes(chunks),
            fail_on_quality_error=fail_on_quality_error,
            sync_to_aws=sync_to_aws,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/status", response_model=PipelineStatusResponse)
def get_pipeline_status(
    run_id: str | None = None,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineStatusResponse:
    """Return aggregate run counters and status for the latest (or specified) run."""
    safe_id = _validate_id(run_id, "run_id") if run_id else None
    overview = service.get_status_overview(run_id=safe_id)
    if safe_id is not None and overview.latest_run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline run not found: {safe_id}",
        )
    return overview


@router.get("/runs", response_model=list[PipelineRunRecord])
def list_pipeline_runs(
    limit: int = Query(50, ge=1, le=500),
    run_status: str | None = Query(default=None, alias="status"),
    service: PipelineService = Depends(get_pipeline_service),
) -> list[PipelineRunRecord]:
    """List pipeline execution manifests newest-first."""
    return service.list_runs(limit=limit, status_filter=run_status)


@router.get("/runs/{run_id}", response_model=PipelineRunRecord)
def get_pipeline_run(
    run_id: str,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineRunRecord:
    """Retrieve a single pipeline run manifest by run_id."""
    safe_id = _validate_id(run_id, "run_id")
    record = service.get_run(safe_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline run not found: {safe_id}",
        )
    return record


@router.get("/runs/{run_id}/layers/{layer}", response_model=LayerPreviewResponse)
def preview_medallion_layer(
    run_id: str,
    layer: str,
    limit: int = Query(100, ge=1, le=1000),
    service: PipelineService = Depends(get_pipeline_service),
) -> LayerPreviewResponse:
    """Preview materialized Parquet rows for a Bronze, Silver, Quarantine, or Gold layer."""
    safe_id = _validate_id(run_id, "run_id")
    try:
        return service.preview_layer(run_id=safe_id, layer=layer, limit=limit)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
