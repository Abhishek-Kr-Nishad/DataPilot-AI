"""Diagnostic AI agent and human approval workflow service."""

from __future__ import annotations

from typing import Any, Literal

from datapilot.agent.approval import ApprovalStore
from datapilot.agent.engine import DiagnosticAgent
from datapilot.agent.tools import DiagnosticToolbox
from datapilot.core.config import AppSettings
from datapilot.metadata.store import MetadataStore
from datapilot.schemas.domain import (
    AgentDiagnosisReport,
    ApprovalDecision,
    RecoveryRecommendation,
    SQLValidationPack,
)


class AgentService:
    """Coordinates Gemini/mock diagnostic runs and human-in-the-loop approvals."""

    def __init__(
        self,
        settings: AppSettings,
        metadata_store: MetadataStore,
        approval_store: ApprovalStore,
        gemini_client: Any | None = None,
    ) -> None:
        self.settings = settings
        self.metadata_store = metadata_store
        self.approval_store = approval_store
        self.gemini_client = gemini_client

    def _resolve_target_run_id(self, run_id: str | None) -> str:
        if run_id is not None:
            if self.metadata_store.get_run(run_id) is None:
                raise FileNotFoundError(f"Pipeline run not found: {run_id}")
            return run_id
        runs = self.metadata_store.list_runs(limit=1)
        if not runs:
            raise FileNotFoundError("No pipeline runs found in metadata store.")
        return runs[0].run_id

    def _build_agent(self) -> DiagnosticAgent:
        return DiagnosticAgent(
            self.settings,
            metadata_store=self.metadata_store,
            approval_store=self.approval_store,
            gemini_client=self.gemini_client,
        )

    def _build_toolbox(self) -> DiagnosticToolbox:
        return DiagnosticToolbox(
            self.settings,
            metadata_store=self.metadata_store,
            approval_store=self.approval_store,
        )

    def diagnose(
        self,
        run_id: str | None = None,
        mode: Literal["auto", "mock", "gemini"] = "auto",
    ) -> AgentDiagnosisReport:
        """Run complete 5-tool diagnosis and persist a pending recovery plan."""
        target_id = self._resolve_target_run_id(run_id)
        return self._build_agent().diagnose_run(run_id=target_id, mode=mode)

    def recommend(
        self,
        run_id: str | None = None,
        mode: Literal["auto", "mock", "gemini"] = "auto",
    ) -> RecoveryRecommendation:
        """Generate a recovery recommendation requiring explicit human approval."""
        target_id = self._resolve_target_run_id(run_id)
        return self._build_agent().recommend_for_run(run_id=target_id, mode=mode)

    def inspect_status(self, run_id: str | None = None) -> dict[str, Any]:
        target_id = self._resolve_target_run_id(run_id)
        return self._build_toolbox().inspect_pipeline_status(target_id)

    def inspect_quality(self, run_id: str | None = None) -> dict[str, Any]:
        target_id = self._resolve_target_run_id(run_id)
        return self._build_toolbox().inspect_quality_report(target_id)

    def generate_sql(
        self,
        run_id: str | None = None,
        database_name: str | None = None,
    ) -> SQLValidationPack:
        target_id = self._resolve_target_run_id(run_id)
        return self._build_toolbox().generate_sql_validation(
            target_id, database_name=database_name
        )

    def list_recommendations(
        self,
        run_id: str | None = None,
        status_filter: str | None = None,
    ) -> list[RecoveryRecommendation]:
        recs = self.approval_store.list_recommendations(run_id=run_id)
        if status_filter:
            target = status_filter.strip().upper()
            recs = [r for r in recs if r.approval_status == target]
        return recs

    def get_recommendation(self, recommendation_id: str) -> RecoveryRecommendation | None:
        return self.approval_store.get_recommendation(recommendation_id)

    def approve(self, recommendation_id: str, reviewer: str, notes: str = "") -> RecoveryRecommendation:
        return self.approval_store.approve(recommendation_id, reviewer=reviewer, notes=notes)

    def reject(self, recommendation_id: str, reviewer: str, notes: str = "") -> RecoveryRecommendation:
        return self.approval_store.reject(recommendation_id, reviewer=reviewer, notes=notes)

    def list_audit_events(self) -> list[ApprovalDecision]:
        return self.approval_store.list_audit_events()
