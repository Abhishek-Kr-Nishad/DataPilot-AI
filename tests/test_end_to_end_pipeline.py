"""End-to-end tests combining PySpark pipeline execution, Quarantine sampling, and DiagnosticAgent."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from datapilot.agent.approval import ApprovalStore
from datapilot.agent.engine import DiagnosticAgent
from datapilot.config import load_settings
from datapilot.data_generator import generate_ecommerce_dataset
from datapilot.pipeline.runner import run_pipeline


def test_full_pipeline_to_agent_diagnosis_and_approval(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "corrupt_e2e.csv",
        num_rows=80,
        seed=99,
        scenario="corrupt_prices",
    )

    exec_res = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="corrupt_prices",
        sync_to_aws=True,  # No creds in test env -> must NOT claim aws_synced=True
        spark=spark_session,
    )
    assert exec_res.run_record.status == "QUALITY_FAILED"
    assert exec_res.run_record.aws_synced is False
    assert any("AWS S3 sync skipped" in note for note in exec_res.run_record.notes)

    agent = DiagnosticAgent(settings)
    diag_report = agent.diagnose_run(run_id=exec_res.run_record.run_id)

    assert diag_report.diagnosis.failure_category == "CORRUPT_OR_INVALID_RECORDS"
    assert diag_report.sql_validation.read_only_verified is True
    assert diag_report.recommendation.requires_human_approval is True
    assert diag_report.recommendation.approval_status == "PENDING_APPROVAL"
    assert diag_report.recommendation.auto_executed is False

    # Verify quarantine Parquet reasons were sampled directly by inspect_quality_report
    q_info = agent.toolbox.inspect_quality_report(exec_res.run_record.run_id)
    assert q_info["found"] is True
    assert len(q_info["top_quarantine_reasons"]) > 0

    # Approve via ApprovalStore
    approval_store = ApprovalStore(settings.local.approvals_dir)
    approved = approval_store.approve(
        diag_report.recommendation.recommendation_id,
        reviewer="lead_data_engineer",
        notes="Verified quarantine rows are isolated test artifacts.",
    )
    assert approved.approval_status == "APPROVED"
