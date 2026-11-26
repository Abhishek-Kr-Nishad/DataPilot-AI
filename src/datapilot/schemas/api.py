"""Pydantic request and response schemas for the DataPilot AI FastAPI service."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from datapilot.data_generator import ScenarioName
from datapilot.schemas.domain import (
    ExecutionMode,
    LayerMetrics,
    PipelineRunRecord,
    QualityReport,
)


class HealthResponse(BaseModel):
    """Service health, database status, and execution mode disclosure."""

    status: str
    service: str
    version: str
    requested_mode: ExecutionMode
    effective_mode: ExecutionMode
    fallback_reason: str | None = None
    database_backend: str
    gemini_configured: bool
    aws: dict[str, object]


class PipelineRunRequest(BaseModel):
    """Payload to generate a synthetic batch and run the Medallion pipeline."""

    scenario: ScenarioName = "clean"
    num_rows: int = Field(default=250, ge=1, le=50000)
    seed: int = Field(default=42, ge=0)
    fail_on_quality_error: bool = True
    sync_to_aws: bool = False


class PipelineRunResponse(BaseModel):
    """Outcome of a pipeline execution request."""

    run_record: PipelineRunRecord
    quality_report: QualityReport | None = None


class PipelineStatusResponse(BaseModel):
    """Current/latest pipeline status summary for monitoring views."""

    has_runs: bool
    latest_run: PipelineRunRecord | None = None
    total_runs: int = 0
    success_count: int = 0
    quality_failed_count: int = 0
    failed_count: int = 0


class LayerPreviewResponse(BaseModel):
    """Paginated preview of a materialized Medallion Parquet layer."""

    run_id: str
    layer: str
    total_rows: int
    returned_rows: int
    columns: list[str]
    records: list[dict[str, Any]]


class AnalyticsSummaryResponse(BaseModel):
    """Aggregated business and pipeline metrics served to React Analytics & Overview."""

    run_id: str | None = None
    has_data: bool = False
    execution_mode: ExecutionMode = "local"
    pipeline_status: str = "NO_RUNS"
    total_records_processed: int = 0
    valid_records: int = 0
    rejected_records: int = 0
    duplicate_records: int = 0
    data_quality_score: float = 0.0
    total_gross_revenue: float = 0.0
    total_net_revenue: float = 0.0
    total_discounts: float = 0.0
    avg_order_value: float = 0.0
    daily_revenue: list[dict[str, Any]] = Field(default_factory=list)
    category_performance: list[dict[str, Any]] = Field(default_factory=list)
    regional_sales: list[dict[str, Any]] = Field(default_factory=list)
    top_customers: list[dict[str, Any]] = Field(default_factory=list)


class DiagnoseRequest(BaseModel):
    """Payload for POST /api/v1/agent/diagnose."""

    run_id: str | None = None
    mode: Literal["auto", "mock", "gemini"] = "auto"


class RecommendRequest(BaseModel):
    """Payload for POST /api/v1/agent/recommend."""

    run_id: str | None = None
    mode: Literal["auto", "mock", "gemini"] = "auto"


class ApprovalDecisionRequest(BaseModel):
    """Human reviewer approval or rejection payload."""

    reviewer: str = Field(min_length=1, max_length=120)
    notes: str = Field(default="", max_length=1000)


class AthenaSQLResponse(BaseModel):
    """Amazon Athena DDL and validation SQL assets."""

    database: str
    scripts: dict[str, str]
    ddl_statements: list[str]


class AWSSyncResponse(BaseModel):
    """Outcome of explicit S3 synchronization for a pipeline run."""

    run_id: str
    aws_synced: bool
    s3_bucket: str
    s3_prefix: str
    layers: dict[str, LayerMetrics]
