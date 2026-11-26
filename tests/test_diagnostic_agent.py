"""Unit tests for the Gemini-powered DiagnosticAgent, tools, SQL guard, and approvals."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from datapilot.agent.approval import ApprovalStore
from datapilot.agent.engine import DiagnosticAgent
from datapilot.agent.tools import DiagnosticToolbox, validate_read_only_sql
from datapilot.core.config import load_settings
from datapilot.database.session import DatabaseManager
from datapilot.metadata.store import SQLAlchemyMetadataStore
from datapilot.schemas.domain import (
    LayerMetrics,
    PipelineRunRecord,
    QualityCheckResult,
    QualityReport,
)


def _seed_run_and_report(
    store: SQLAlchemyMetadataStore,
    run_id: str,
    status: str,
    overall_passed: bool,
    error_type: str | None = None,
    error_message: str | None = None,
    missing_cols: list[str] | None = None,
    failed_checks: list[QualityCheckResult] | None = None,
) -> None:
    run = PipelineRunRecord(
        run_id=run_id,
        batch_id=run_id,
        status=status,  # type: ignore[arg-type]
        requested_mode="local",
        effective_mode="local",
        source_file="data/raw/orders.csv",
        scenario="test",
        layers={
            "bronze": LayerMetrics(layer="bronze", path="data/bronze", row_count=100, column_count=18),
        },
        error_type=error_type,
        error_message=error_message,
    )
    store.save_run(run)

    report = QualityReport(
        run_id=run_id,
        dataset_name="orders.csv",
        overall_passed=overall_passed,
        quality_score=100.0 if overall_passed else 45.0,
        total_bronze_rows=100,
        valid_silver_rows=100 if overall_passed else 65,
        quarantined_rows=0 if overall_passed else 20,
        duplicate_rows_removed=0 if overall_passed else 15,
        schema_valid=not bool(missing_cols),
        missing_columns=missing_cols or [],
        null_counts={"customer_id": 0 if overall_passed else 14},
        null_rates={"customer_id": 0.0 if overall_passed else 0.14},
        checks=failed_checks or [],
        summary="PASSED" if overall_passed else "FAILED quality gate",
    )
    store.save_quality_report(report)


def test_read_only_sql_validator_blocks_mutations() -> None:
    assert validate_read_only_sql("SELECT COUNT(*) FROM db.silver_orders;") is True
    assert validate_read_only_sql("SHOW COLUMNS IN db.bronze_orders;") is True

    for unsafe in [
        "DROP TABLE db.silver_orders;",
        "DELETE FROM db.silver_orders WHERE 1=1;",
        "INSERT INTO db.gold_daily_revenue VALUES (1);",
        "ALTER TABLE db.bronze_orders ADD COLUMNS (x string);",
    ]:
        with pytest.raises(ValueError):
            validate_read_only_sql(unsafe)


def test_toolbox_rejects_unsafe_run_ids(tmp_path: Path) -> None:
    settings = load_settings(project_root=tmp_path)
    settings.ensure_directories()
    toolbox = DiagnosticToolbox(settings)

    with pytest.raises(ValueError):
        toolbox.inspect_pipeline_status(run_id="../secrets")
    with pytest.raises(ValueError):
        toolbox.inspect_pipeline_status(run_id="run; DROP TABLE x")


def test_agent_diagnoses_schema_drift_and_requires_human_approval(tmp_path: Path) -> None:
    settings = load_settings(project_root=tmp_path)
    settings.ensure_directories()
    db_mgr = DatabaseManager(f"sqlite:///{(tmp_path / 'agent.db').as_posix()}")
    store = SQLAlchemyMetadataStore(db_mgr, reports_dir=settings.local.reports_dir)
    approval_store = ApprovalStore(settings.local.approvals_dir, db_manager=db_mgr)

    _seed_run_and_report(
        store,
        run_id="run_drift_01",
        status="FAILED",
        overall_passed=False,
        error_type="SchemaDriftError",
        error_message="Missing customer_id and unit_price",
        missing_cols=["customer_id", "unit_price"],
        failed_checks=[
            QualityCheckResult(
                check_name="schema_contract",
                passed=False,
                severity="ERROR",
                metric_value=2,
                threshold="0 missing",
                affected_rows=100,
                details="Missing customer_id, unit_price",
            )
        ],
    )

    agent = DiagnosticAgent(settings, metadata_store=store, approval_store=approval_store)
    report = agent.diagnose_run(run_id="run_drift_01", mode="mock")

    assert report.llm_provider == "mock"
    assert report.diagnosis.failure_category == "SCHEMA_DRIFT"
    assert report.diagnosis.severity == "CRITICAL"
    assert "schema_inspection" in report.sql_validation.queries
    assert report.sql_validation.read_only_verified is True
    assert report.recommendation.requires_human_approval is True
    assert report.recommendation.approval_status == "PENDING_APPROVAL"
    assert report.recommendation.auto_executed is False
    assert len(report.tool_trace) >= 5

    with pytest.raises(ValueError):
        approval_store.approve(report.recommendation.recommendation_id, reviewer="   ")

    approved = approval_store.approve(
        report.recommendation.recommendation_id,
        reviewer="abhishek.nishad",
        notes="Confirmed upstream export fix; safe to re-run.",
    )
    assert approved.approval_status == "APPROVED"
    assert approved.approved_by == "abhishek.nishad"
    assert len(approval_store.list_audit_events()) == 1

    with pytest.raises(ValueError):
        approval_store.reject(report.recommendation.recommendation_id, reviewer="other")


def test_agent_diagnoses_null_spike_and_duplicates_and_supports_reject(tmp_path: Path) -> None:
    settings = load_settings(project_root=tmp_path)
    settings.ensure_directories()
    db_mgr = DatabaseManager(f"sqlite:///{(tmp_path / 'agent2.db').as_posix()}")
    store = SQLAlchemyMetadataStore(db_mgr, reports_dir=settings.local.reports_dir)
    approval_store = ApprovalStore(settings.local.approvals_dir, db_manager=db_mgr)

    _seed_run_and_report(
        store,
        run_id="run_dirty_02",
        status="QUALITY_FAILED",
        overall_passed=False,
        error_type="DataQualityGateError",
        error_message="FAILED quality gate",
        failed_checks=[
            QualityCheckResult(
                check_name="critical_null_rate",
                passed=False,
                severity="ERROR",
                metric_value=0.14,
                threshold="<= 0.05",
                affected_rows=14,
                details="customer_id null rate 14%",
            ),
            QualityCheckResult(
                check_name="duplicate_order_rate",
                passed=False,
                severity="ERROR",
                metric_value=0.15,
                threshold="<= 0.05",
                affected_rows=15,
                details="15 duplicate order_id rows",
            ),
        ],
    )

    agent = DiagnosticAgent(settings, metadata_store=store, approval_store=approval_store)
    report = agent.diagnose_run(run_id="run_dirty_02", mode="mock")

    assert report.diagnosis.failure_category == "NULL_SPIKE_AND_DUPLICATES"
    assert report.recommendation.approval_status == "PENDING_APPROVAL"

    rejected = approval_store.reject(
        report.recommendation.recommendation_id,
        reviewer="data_steward",
        notes="Hold batch until vendor replays clean extract.",
    )
    assert rejected.approval_status == "REJECTED"
    assert rejected.approved_by == "data_steward"


def test_agent_gemini_mode_with_mocked_client_and_missing_key_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    settings = load_settings(project_root=tmp_path)
    settings.ensure_directories()
    db_mgr = DatabaseManager(f"sqlite:///{(tmp_path / 'gemini.db').as_posix()}")
    store = SQLAlchemyMetadataStore(db_mgr, reports_dir=settings.local.reports_dir)
    approval_store = ApprovalStore(settings.local.approvals_dir, db_manager=db_mgr)

    _seed_run_and_report(
        store,
        run_id="run_corrupt_gem",
        status="QUALITY_FAILED",
        overall_passed=False,
        error_type="DataQualityGateError",
        error_message="High quarantine ratio",
        failed_checks=[
            QualityCheckResult(
                check_name="quarantine_ratio",
                passed=False,
                severity="ERROR",
                metric_value=0.35,
                threshold="<= 0.15",
                affected_rows=35,
                details="35% quarantined",
            )
        ],
    )

    # Strict gemini mode without key or injected client raises ValueError
    agent_no_key = DiagnosticAgent(settings, metadata_store=store, approval_store=approval_store)
    with pytest.raises(ValueError):
        agent_no_key.diagnose_run("run_corrupt_gem", mode="gemini")

    # Mocked Gemini client verifies tool registration & grounded synthesis without network calls
    captured_calls: list[dict] = []

    class _FakeModels:
        def generate_content(self, *, model, contents, config):
            captured_calls.append({"model": model, "contents": contents, "tools": config.tools})
            return SimpleNamespace(
                text="Mocked Gemini Summary: 35% of orders quarantined; await human approval."
            )

    fake_gemini = SimpleNamespace(models=_FakeModels())
    agent_mocked_gemini = DiagnosticAgent(
        settings,
        metadata_store=store,
        approval_store=approval_store,
        gemini_client=fake_gemini,
    )
    report = agent_mocked_gemini.diagnose_run("run_corrupt_gem", mode="gemini")
    assert report.llm_provider == "gemini"
    assert report.llm_enhanced is True
    assert report.llm_model == settings.gemini_model
    assert "Mocked Gemini Summary" in report.narrative_summary
    assert len(captured_calls) == 1
    assert len(captured_calls[0]["tools"]) == 5
