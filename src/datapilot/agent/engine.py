"""Gemini-powered DiagnosticAgent orchestrating the five validated tools."""

from __future__ import annotations

import os
from typing import Any, Literal

from datapilot.agent.approval import ApprovalStore
from datapilot.agent.tools import DiagnosticToolbox
from datapilot.core.config import AppSettings
from datapilot.core.logging import get_logger
from datapilot.metadata.store import MetadataStore
from datapilot.schemas.domain import (
    AgentDiagnosisReport,
    FailureDiagnosis,
    RecoveryRecommendation,
    SQLValidationPack,
)


class DiagnosticAgent:
    """Agentic AI diagnostician powered by Google Gemini tool-calling + human approval.

    Supports:
    - ``mock`` mode: 100% deterministic, offline tool execution with zero API calls.
    - ``gemini`` mode: Uses ``google.genai.Client`` with function tools and structured
      grounding (requires ``GEMINI_API_KEY`` or an injected ``gemini_client``).
    - ``auto`` mode: Uses Gemini when ``GEMINI_API_KEY`` (or injected client) is
      available; otherwise executes cleanly in ``mock`` mode.

    Never executes destructive actions automatically.
    """

    def __init__(
        self,
        settings: AppSettings,
        metadata_store: MetadataStore | None = None,
        approval_store: ApprovalStore | None = None,
        gemini_client: Any | None = None,
    ) -> None:
        self.settings = settings
        self.toolbox = DiagnosticToolbox(
            settings=settings,
            metadata_store=metadata_store,
            approval_store=approval_store,
        )
        self._injected_gemini_client = gemini_client
        self.logger = get_logger(
            "datapilot.agent.engine",
            level=settings.local.log_level,
            log_file=settings.local.logs_dir / "agent_engine.log",
        )

    def _build_deterministic_narrative(
        self,
        status_info: dict[str, Any],
        quality_info: dict[str, Any],
        diagnosis: FailureDiagnosis,
        sql_pack: SQLValidationPack,
        recommendation: RecoveryRecommendation,
    ) -> str:
        lines = [
            f"Run `{diagnosis.run_id}` finished with status `{diagnosis.status_observed}` "
            f"(mode={status_info.get('effective_mode', 'local')}).",
            f"Diagnosis [{diagnosis.failure_category}, severity={diagnosis.severity}, "
            f"confidence={diagnosis.confidence:.0%}]: {diagnosis.root_cause}",
        ]
        if quality_info.get("found"):
            lines.append(f"Quality Summary: {quality_info.get('summary')}")
        lines.append(
            f"Generated {len(sql_pack.queries)} read-only Athena validation queries "
            f"(read_only_verified={sql_pack.read_only_verified})."
        )
        lines.append(
            f"Recovery proposal `{recommendation.recommendation_id}` contains "
            f"{len(recommendation.actions)} step(s) and is currently "
            f"`{recommendation.approval_status}` (human approval required; no changes auto-executed)."
        )
        return " ".join(lines)

    def _build_gemini_tools(self) -> list[Any]:
        """Create bounded, read-only/advisory Python tool wrappers for Gemini."""

        def inspect_pipeline_status(run_id: str) -> dict[str, Any]:
            """Inspect pipeline run manifest, status, layers, and error log tail."""
            return self.toolbox.inspect_pipeline_status(run_id=run_id)

        def inspect_quality_report(run_id: str) -> dict[str, Any]:
            """Inspect quality assertions, null rates, and quarantine counts."""
            return self.toolbox.inspect_quality_report(run_id=run_id)

        def identify_failure_reason(run_id: str) -> dict[str, Any]:
            """Classify failure category, root cause, and affected layers."""
            return self.toolbox.identify_failure_reason(run_id=run_id).model_dump()

        def generate_sql_validation(run_id: str) -> dict[str, Any]:
            """Generate read-only Amazon Athena validation queries for the run."""
            return self.toolbox.generate_sql_validation(run_id=run_id).model_dump()

        def recommend_recovery(run_id: str) -> dict[str, Any]:
            """Create a non-destructive recovery plan requiring human approval."""
            return self.toolbox.recommend_recovery(run_id=run_id).model_dump()

        return [
            inspect_pipeline_status,
            inspect_quality_report,
            identify_failure_reason,
            generate_sql_validation,
            recommend_recovery,
        ]

    def _run_gemini_synthesis(
        self,
        resolved_run_id: str,
        base_narrative: str,
        diagnosis: FailureDiagnosis,
        quality_info: dict[str, Any],
        sql_pack: SQLValidationPack,
        recommendation: RecoveryRecommendation,
        strict: bool,
    ) -> tuple[str, bool, str | None]:
        api_key = os.getenv("GEMINI_API_KEY", "").strip()
        client = self._injected_gemini_client

        if client is None:
            if not api_key:
                if strict:
                    raise ValueError(
                        "GEMINI_API_KEY is required when agent mode='gemini' without a mock client."
                    )
                return base_narrative, False, None
            from google import genai

            client = genai.Client(api_key=api_key)

        try:
            from google.genai import types

            prompt = (
                "You are DataPilot AI, a senior AWS Data Engineering & PySpark diagnostic agent. "
                "Using ONLY the verified tool findings below (do not invent numbers or execute changes), "
                "write a crisp 3-sentence executive incident summary and next-step guidance noting that "
                "human approval is required.\n\n"
                f"Run ID: {resolved_run_id}\n"
                f"Failure Category: {diagnosis.failure_category}\n"
                f"Severity: {diagnosis.severity}\n"
                f"Root Cause: {diagnosis.root_cause}\n"
                f"Evidence: {diagnosis.evidence}\n"
                f"Quality Summary: {quality_info.get('summary', 'N/A')}\n"
                f"SQL Queries Generated: {list(sql_pack.queries.keys())}\n"
                f"Pending Recommendation ID: {recommendation.recommendation_id} "
                f"(status={recommendation.approval_status})\n"
                f"Baseline Summary: {base_narrative}"
            )
            config = types.GenerateContentConfig(
                system_instruction=(
                    "Explain data pipeline diagnostics concisely for data engineers. "
                    "Never recommend auto-executing destructive commands without human approval."
                ),
                tools=self._build_gemini_tools(),
                temperature=0.1,
                max_output_tokens=300,
            )
            response = client.models.generate_content(
                model=self.settings.gemini_model,
                contents=prompt,
                config=config,
            )
            text = (getattr(response, "text", None) or "").strip()
            if text:
                return text, True, self.settings.gemini_model
        except Exception as exc:
            self.logger.warning("Gemini synthesis failed; returning deterministic output: %s", exc)
            if strict:
                raise

        return base_narrative, False, None

    def diagnose_run(
        self,
        run_id: str | None = None,
        mode: Literal["auto", "mock", "gemini"] | None = None,
    ) -> AgentDiagnosisReport:
        """Run the full 5-tool diagnostic workflow using Gemini or deterministic mock mode."""
        self.toolbox.tool_trace.clear()
        effective_mode = mode or self.settings.agent_mode
        self.logger.info(
            "Starting DiagnosticAgent inspection for run_id=%s mode=%s",
            run_id or "latest",
            effective_mode,
        )

        status_info = self.toolbox.inspect_pipeline_status(run_id=run_id)
        resolved_run_id = str(status_info.get("run_id") or (run_id or "none"))
        quality_info = self.toolbox.inspect_quality_report(run_id=resolved_run_id)
        diagnosis = self.toolbox.identify_failure_reason(run_id=resolved_run_id)
        sql_pack = self.toolbox.generate_sql_validation(run_id=resolved_run_id)
        recommendation = self.toolbox.recommend_recovery(run_id=resolved_run_id)

        base_narrative = self._build_deterministic_narrative(
            status_info, quality_info, diagnosis, sql_pack, recommendation
        )

        narrative = base_narrative
        llm_enhanced = False
        llm_provider: Literal["mock", "gemini"] = "mock"
        llm_model: str | None = None

        if effective_mode in ("auto", "gemini"):
            narrative, llm_enhanced, llm_model = self._run_gemini_synthesis(
                resolved_run_id=resolved_run_id,
                base_narrative=base_narrative,
                diagnosis=diagnosis,
                quality_info=quality_info,
                sql_pack=sql_pack,
                recommendation=recommendation,
                strict=(effective_mode == "gemini"),
            )
            if llm_enhanced:
                llm_provider = "gemini"

        return AgentDiagnosisReport(
            run_id=resolved_run_id,
            execution_mode=self.settings.effective_mode,
            llm_provider=llm_provider,
            llm_model=llm_model,
            pipeline_status=str(status_info.get("status", "UNKNOWN")),
            quality_summary=str(quality_info.get("summary") or quality_info.get("message", "")),
            diagnosis=diagnosis,
            sql_validation=sql_pack,
            recommendation=recommendation,
            llm_enhanced=llm_enhanced,
            narrative_summary=narrative,
            tool_trace=list(self.toolbox.tool_trace),
        )

    def recommend_for_run(
        self,
        run_id: str | None = None,
        mode: Literal["auto", "mock", "gemini"] | None = None,
    ) -> RecoveryRecommendation:
        """Generate and persist a pending human-approval recovery recommendation."""
        report = self.diagnose_run(run_id=run_id, mode=mode)
        return report.recommendation
