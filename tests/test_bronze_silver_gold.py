"""Tests for Bronze, Silver, Gold PySpark transformations and data quality checks."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from datapilot.config import load_settings
from datapilot.data_generator import generate_ecommerce_dataset
from datapilot.pipeline.runner import run_pipeline


def test_clean_pipeline_end_to_end(spark_session: SparkSession, tmp_path: Path) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "clean.csv",
        num_rows=120,
        seed=42,
        scenario="clean",
    )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="clean",
        spark=spark_session,
    )
    rec = result.run_record
    assert rec.status == "SUCCESS"
    assert rec.effective_mode == "local"
    assert rec.aws_synced is False
    assert rec.layers["bronze"].row_count == 120
    assert rec.layers["silver"].row_count == 120
    assert rec.layers["quarantine"].row_count == 0
    assert "gold_daily_revenue" in rec.layers
    assert "gold_category_performance" in rec.layers
    assert "gold_regional_sales" in rec.layers
    assert "gold_customer_ltv_summary" in rec.layers
    assert result.quality_report is not None
    assert result.quality_report.overall_passed is True


def test_dirty_nulls_and_duplicates_quarantined_and_gated(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "dirty.csv",
        num_rows=100,
        seed=7,
        scenario="dirty_nulls_duplicates",
    )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="dirty_nulls_duplicates",
        spark=spark_session,
    )
    rec = result.run_record
    report = result.quality_report

    assert rec.status == "QUALITY_FAILED"
    assert report is not None
    assert report.overall_passed is False
    assert report.quarantined_rows > 0
    assert report.duplicate_rows_removed > 0
    assert "gold_daily_revenue" not in rec.layers


def test_corrupt_prices_and_dates_route_safely_to_quarantine(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "corrupt.csv",
        num_rows=80,
        seed=11,
        scenario="corrupt_prices",
    )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="corrupt_prices",
        spark=spark_session,
    )
    assert result.run_record.status == "QUALITY_FAILED"
    assert result.quality_report is not None
    assert result.quality_report.quarantined_rows >= 15
    assert result.quality_report.valid_silver_rows > 0


def test_schema_drift_triggers_structured_failure_and_report(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "drift.csv",
        num_rows=50,
        seed=5,
        scenario="schema_drift",
    )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="schema_drift",
        spark=spark_session,
    )
    rec = result.run_record
    report = result.quality_report

    assert rec.status == "FAILED"
    assert rec.error_type == "SchemaDriftError"
    assert report is not None
    assert report.schema_valid is False
    assert "customer_id" in report.missing_columns
    assert "unit_price" in report.missing_columns
    assert "legacy_promo_flag" in report.unexpected_columns


def test_empty_file_fails_minimum_row_count_gate(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    settings = load_settings(project_root=tmp_path)
    raw_csv = generate_ecommerce_dataset(
        tmp_path / "data" / "raw" / "empty.csv",
        scenario="empty_file",
    )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=raw_csv,
        scenario="empty_file",
        spark=spark_session,
    )
    assert result.run_record.status == "QUALITY_FAILED"
    assert result.quality_report is not None
    assert result.quality_report.total_bronze_rows == 0
    failed = {c.check_name for c in result.quality_report.checks if not c.passed}
    assert "minimum_row_count" in failed
    assert "silver_non_empty" in failed
