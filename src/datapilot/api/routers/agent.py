"""Agentic AI diagnostic, recommendation, SQL validation, and human approval endpoints."""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from datapilot.dependencies.container import AgentService, get_agent_service
from datapilot.schemas.api import ApprovalDecisionRequest, DiagnoseRequest, RecommendRequest
from datapilot.schemas.domain import (
    AgentDiagnosisReport,
    ApprovalDecision,
    RecoveryRecommendation,
    SQLValidationPack,
)

router = APIRouter(prefix="/api/v1/agent", tags=["Agentic AI"])
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _validate_id(value: str, label: str) -> str:
    cleaned = value.strip()
    if not cleaned or not _SAFE_ID_RE.match(cleaned):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{label} contains invalid characters: {value!r}",
        )
    return cleaned


@router.post("/diagnose", response_model=AgentDiagnosisReport)
def diagnose_pipeline_run(
    payload: DiagnoseRequest | None = None,
    service: AgentService = Depends(get_agent_service),
) -> AgentDiagnosisReport:
    """Run the 5-tool Gemini/mock diagnostic workflow on the specified or latest run."""
    req = payload or DiagnoseRequest()
    safe_id = _validate_id(req.run_id, "run_id") if req.run_id else None
    try:
        return service.diagnose(run_id=safe_id, mode=req.mode)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post("/recommend", response_model=RecoveryRecommendation)
def recommend_pipeline_recovery(
    payload: RecommendRequest | None = None,
    service: AgentService = Depends(get_agent_service),
) -> RecoveryRecommendation:
    """Generate a non-destructive recovery recommendation requiring human approval."""
    req = payload or RecommendRequest()
    safe_id = _validate_id(req.run_id, "run_id") if req.run_id else None
    try:
        return service.recommend(run_id=safe_id, mode=req.mode)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/tools/status")
def inspect_status_tool(
    run_id: str | None = None,
    service: AgentService = Depends(get_agent_service),
) -> dict[str, Any]:
    """Read-only tool endpoint: inspect_pipeline_status()."""
    safe_id = _validate_id(run_id, "run_id") if run_id else None
    try:
        return service.inspect_status(run_id=safe_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/tools/quality")
def inspect_quality_tool(
    run_id: str | None = None,
    service: AgentService = Depends(get_agent_service),
) -> dict[str, Any]:
    """Read-only tool endpoint: inspect_quality_report()."""
    safe_id = _validate_id(run_id, "run_id") if run_id else None
    try:
        return service.inspect_quality(run_id=safe_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/tools/sql", response_model=SQLValidationPack)
def generate_sql_tool(
    run_id: str | None = None,
    database_name: str | None = None,
    service: AgentService = Depends(get_agent_service),
) -> SQLValidationPack:
    """Read-only tool endpoint: generate_sql_validation()."""
    safe_id = _validate_id(run_id, "run_id") if run_id else None
    try:
        return service.generate_sql(run_id=safe_id, database_name=database_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


@router.get("/recommendations", response_model=list[RecoveryRecommendation])
def list_recommendations(
    run_id: str | None = None,
    approval_status: str | None = Query(default=None, alias="status"),
    service: AgentService = Depends(get_agent_service),
) -> list[RecoveryRecommendation]:
    """List recovery recommendations awaiting or completed human review."""
    safe_id = _validate_id(run_id, "run_id") if run_id else None
    return service.list_recommendations(run_id=safe_id, status_filter=approval_status)


@router.get("/approvals/audit", response_model=list[ApprovalDecision])
def list_approval_audit_events(
    service: AgentService = Depends(get_agent_service),
) -> list[ApprovalDecision]:
    """Return immutable human approval audit trail events."""
    return service.list_audit_events()


@router.post("/approvals/{recommendation_id}/approve", response_model=RecoveryRecommendation)
def approve_recommendation(
    recommendation_id: str,
    payload: ApprovalDecisionRequest,
    service: AgentService = Depends(get_agent_service),
) -> RecoveryRecommendation:
    """Record explicit human approval for a pending recovery plan."""
    safe_rec_id = _validate_id(recommendation_id, "recommendation_id")
    try:
        return service.approve(safe_rec_id, reviewer=payload.reviewer, notes=payload.notes)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        code = (
            status.HTTP_409_CONFLICT
            if "already decided" in str(exc)
            else status.HTTP_422_UNPROCESSABLE_ENTITY
        )
        raise HTTPException(status_code=code, detail=str(exc)) from exc


@router.post("/approvals/{recommendation_id}/reject", response_model=RecoveryRecommendation)
def reject_recommendation(
    recommendation_id: str,
    payload: ApprovalDecisionRequest,
    service: AgentService = Depends(get_agent_service),
) -> RecoveryRecommendation:
    """Record explicit human rejection for a pending recovery plan."""
    safe_rec_id = _validate_id(recommendation_id, "recommendation_id")
    try:
        return service.reject(safe_rec_id, reviewer=payload.reviewer, notes=payload.notes)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        code = (
            status.HTTP_409_CONFLICT
            if "already decided" in str(exc)
            else status.HTTP_422_UNPROCESSABLE_ENTITY
        )
        raise HTTPException(status_code=code, detail=str(exc)) from exc
