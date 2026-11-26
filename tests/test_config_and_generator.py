"""Unit tests for configuration, SQLAlchemy metadata store, local storage, and data generation."""

from __future__ import annotations

from pathlib import Path

import pytest

from datapilot.core.config import ConfigurationError, load_settings
from datapilot.core.logging import redact_secrets
from datapilot.data_generator import EXPECTED_RAW_COLUMNS, generate_ecommerce_dataset
from datapilot.database.session import DatabaseManager
from datapilot.metadata.store import LocalMetadataStore, SQLAlchemyMetadataStore
from datapilot.schemas.domain import PipelineRunRecord, QualityReport
from datapilot.storage.local_storage import LocalStorageBackend


def test_load_settings_defaults_to_local(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAPILOT_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("AWS_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("DATAPILOT_DATA_DIR", str(tmp_path / "data"))

    settings = load_settings(project_root=tmp_path)
    settings.ensure_directories()

    assert settings.requested_mode == "local"
    assert settings.effective_mode == "local"
    assert settings.fallback_reason is None
    assert settings.metadata.backend == "sqlalchemy"
    assert (tmp_path / "data" / "bronze").exists()
    assert (tmp_path / "data" / "silver" / "quarantine").exists()
    assert (tmp_path / "data" / "gold").exists()


def test_aws_mode_falls_back_truthfully_when_unconfigured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATAPILOT_EXECUTION_MODE", "aws")
    monkeypatch.delenv("AWS_S3_BUCKET", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    settings = load_settings(project_root=tmp_path, strict_aws=False)
    assert settings.requested_mode == "aws"
    assert settings.effective_mode == "local"
    assert settings.fallback_reason is not None

    with pytest.raises(ConfigurationError):
        load_settings(project_root=tmp_path, strict_aws=True)


def test_secret_redaction_covers_aws_gemini_and_postgres() -> None:
    sample = (
        "Leak AKIAIOSFODNN7EXAMPLE aws_secret_access_key=supersecret123 "
        "GEMINI_API_KEY=AIzaSyDummySecretKey1234567890 "
        "postgresql://admin:dbpass123@localhost:5432/datapilot"
    )
    cleaned = redact_secrets(sample)
    assert "AKIAIOSFODNN7EXAMPLE" not in cleaned
    assert "supersecret123" not in cleaned
    assert "AIzaSyDummySecretKey1234567890" not in cleaned
    assert "dbpass123" not in cleaned
    assert "[REDACTED_AWS_KEY]" in cleaned
    assert "[REDACTED_GEMINI_KEY]" in cleaned


def test_data_generator_is_reproducible_and_supports_scenarios(tmp_path: Path) -> None:
    csv_a = generate_ecommerce_dataset(tmp_path / "a.csv", num_rows=60, seed=123, scenario="clean")
    csv_b = generate_ecommerce_dataset(tmp_path / "b.csv", num_rows=60, seed=123, scenario="clean")
    assert csv_a.read_bytes() == csv_b.read_bytes()

    header = csv_a.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert header == EXPECTED_RAW_COLUMNS

    drift_csv = generate_ecommerce_dataset(
        tmp_path / "drift.csv", num_rows=20, seed=99, scenario="schema_drift"
    )
    drift_header = drift_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert "customer_id" not in drift_header
    assert "unit_price" not in drift_header
    assert "item_cost_usd" in drift_header
    assert "legacy_promo_flag" in drift_header

    empty_csv = generate_ecommerce_dataset(tmp_path / "empty.csv", scenario="empty_file")
    assert len(empty_csv.read_text(encoding="utf-8").splitlines()) == 1


def test_sqlalchemy_and_local_metadata_store_and_storage(tmp_path: Path) -> None:
    db_mgr = DatabaseManager(f"sqlite:///{(tmp_path / 'meta.db').as_posix()}")
    sql_store = SQLAlchemyMetadataStore(db_mgr, reports_dir=tmp_path / "reports")

    run = PipelineRunRecord(
        run_id="run-001",
        batch_id="batch-001",
        status="SUCCESS",
        requested_mode="local",
        effective_mode="local",
        source_file="orders.csv",
    )
    sql_store.save_run(run)
    loaded = sql_store.get_run("run-001")
    assert loaded is not None
    assert loaded.run_id == "run-001"
    assert len(sql_store.list_runs()) == 1

    report = QualityReport(
        run_id="run-001",
        dataset_name="orders.csv",
        overall_passed=True,
        quality_score=100.0,
        total_bronze_rows=50,
        valid_silver_rows=50,
        quarantined_rows=0,
        duplicate_rows_removed=0,
        schema_valid=True,
        summary="All checks passed.",
    )
    sql_store.save_quality_report(report)
    loaded_report = sql_store.get_quality_report("run-001")
    assert loaded_report is not None
    assert loaded_report.overall_passed is True
    assert loaded_report.quality_score == 100.0

    # Verify JSON mirror also persisted
    json_store = LocalMetadataStore(tmp_path / "reports")
    assert json_store.get_run("run-001") is not None

    storage = LocalStorageBackend(tmp_path / "storage")
    sample_file = tmp_path / "sample.txt"
    sample_file.write_text("hello", encoding="utf-8")
    storage.upload_file(sample_file, "bronze/sample.txt")
    assert storage.exists("bronze/sample.txt")
    assert storage.list_objects("bronze") == ["bronze/sample.txt"]
