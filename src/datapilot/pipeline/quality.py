"""Data quality validation engine generating structured JSON reports."""

from __future__ import annotations

from datapilot.config import QualityThresholdConfig
from datapilot.metadata.models import (
    ExecutionMode,
    QualityCheckResult,
    QualityReport,
    compute_quality_score,
)
from datapilot.pipeline.schemas import SchemaValidationResult
from datapilot.pipeline.silver import SilverResult


def evaluate_data_quality(
    run_id: str,
    dataset_name: str,
    silver_result: SilverResult,
    thresholds: QualityThresholdConfig,
    execution_mode: ExecutionMode = "local",
) -> QualityReport:
    """Evaluate deterministic data quality rules on a completed Silver run."""
    total_bronze = silver_result.bronze_row_count
    valid_silver = silver_result.silver_row_count
    quarantined = silver_result.quarantined_row_count
    duplicates = silver_result.duplicate_rows_removed
    schema_val: SchemaValidationResult = silver_result.schema_validation

    null_rates: dict[str, float] = {}
    if total_bronze > 0:
        null_rates = {
            col_name: round(count / total_bronze, 4)
            for col_name, count in silver_result.null_counts.items()
        }
    max_observed_null_rate = max(null_rates.values(), default=0.0)
    worst_null_col = (
        max(null_rates.items(), key=lambda item: item[1])[0] if null_rates else "none"
    )

    duplicate_rate = round(duplicates / total_bronze, 4) if total_bronze > 0 else 0.0
    quarantine_rate = round(quarantined / total_bronze, 4) if total_bronze > 0 else 0.0

    checks: list[QualityCheckResult] = [
        QualityCheckResult(
            check_name="schema_contract",
            passed=len(schema_val.missing_columns) == 0,
            severity="ERROR",
            metric_value=len(schema_val.missing_columns),
            threshold="0 missing required columns",
            affected_rows=total_bronze if schema_val.missing_columns else 0,
            details=(
                "All required columns present."
                if not schema_val.missing_columns
                else f"Missing required columns: {schema_val.missing_columns}"
            ),
        ),
        QualityCheckResult(
            check_name="unexpected_columns_check",
            passed=len(schema_val.unexpected_columns) == 0,
            severity="WARNING",
            metric_value=len(schema_val.unexpected_columns),
            threshold="0 unexpected columns",
            affected_rows=0,
            details=(
                "No unexpected columns detected."
                if not schema_val.unexpected_columns
                else f"Unexpected columns present: {schema_val.unexpected_columns}"
            ),
        ),
        QualityCheckResult(
            check_name="minimum_row_count",
            passed=total_bronze >= thresholds.min_row_count,
            severity="ERROR",
            metric_value=total_bronze,
            threshold=f">= {thresholds.min_row_count}",
            affected_rows=0 if total_bronze >= thresholds.min_row_count else 1,
            details=f"Ingested {total_bronze} raw records from {dataset_name}.",
        ),
        QualityCheckResult(
            check_name="critical_null_rate",
            passed=max_observed_null_rate <= thresholds.max_null_rate,
            severity="ERROR",
            metric_value=max_observed_null_rate,
            threshold=f"<= {thresholds.max_null_rate}",
            affected_rows=silver_result.null_counts.get(worst_null_col, 0),
            details=(
                f"Highest null rate in '{worst_null_col}' = {max_observed_null_rate:.2%} "
                f"(threshold {thresholds.max_null_rate:.2%})."
            ),
        ),
        QualityCheckResult(
            check_name="duplicate_order_rate",
            passed=duplicate_rate <= thresholds.max_duplicate_rate,
            severity="ERROR",
            metric_value=duplicate_rate,
            threshold=f"<= {thresholds.max_duplicate_rate}",
            affected_rows=duplicates,
            details=(
                f"Removed {duplicates} duplicate order_id rows ({duplicate_rate:.2%} of Bronze)."
            ),
        ),
        QualityCheckResult(
            check_name="quarantine_ratio",
            passed=quarantine_rate <= thresholds.max_quarantine_rate,
            severity="ERROR",
            metric_value=quarantine_rate,
            threshold=f"<= {thresholds.max_quarantine_rate}",
            affected_rows=quarantined,
            details=(
                f"Quarantined {quarantined} invalid rows ({quarantine_rate:.2%} of Bronze)."
            ),
        ),
        QualityCheckResult(
            check_name="silver_non_empty",
            passed=valid_silver > 0,
            severity="ERROR",
            metric_value=valid_silver,
            threshold="> 0 valid Silver records",
            affected_rows=0 if valid_silver > 0 else total_bronze,
            details=f"Produced {valid_silver} clean deduplicated Silver records.",
        ),
    ]

    blocking_failures = [c for c in checks if not c.passed and c.severity == "ERROR"]
    overall_passed = len(blocking_failures) == 0

    if overall_passed:
        summary = (
            f"PASSED: {valid_silver}/{total_bronze} rows promoted to Silver "
            f"({duplicates} duplicates removed, {quarantined} quarantined)."
        )
    else:
        failed_names = ", ".join(c.check_name for c in blocking_failures)
        summary = (
            f"FAILED quality gate on [{failed_names}]: "
            f"bronze={total_bronze}, silver={valid_silver}, "
            f"quarantined={quarantined}, duplicates={duplicates}."
        )

    quality_score = compute_quality_score(
        total_bronze_rows=total_bronze,
        valid_silver_rows=valid_silver,
        checks=checks,
        schema_valid=schema_val.schema_valid,
    )

    return QualityReport(
        run_id=run_id,
        dataset_name=dataset_name,
        execution_mode=execution_mode,
        overall_passed=overall_passed,
        quality_score=quality_score,
        total_bronze_rows=total_bronze,
        valid_silver_rows=valid_silver,
        quarantined_rows=quarantined,
        duplicate_rows_removed=duplicates,
        schema_valid=schema_val.schema_valid,
        missing_columns=schema_val.missing_columns,
        unexpected_columns=schema_val.unexpected_columns,
        null_counts=silver_result.null_counts,
        null_rates=null_rates,
        quarantine_reason_counts=silver_result.quarantine_reason_counts,
        checks=checks,
        summary=summary,
    )


def build_schema_drift_quality_report(
    run_id: str,
    dataset_name: str,
    schema_val: SchemaValidationResult,
    bronze_row_count: int,
    execution_mode: ExecutionMode = "local",
) -> QualityReport:
    """Create a diagnostic QualityReport when schema drift aborts Silver transforms."""
    check = QualityCheckResult(
        check_name="schema_contract",
        passed=False,
        severity="ERROR",
        metric_value=len(schema_val.missing_columns),
        threshold="0 missing required columns",
        affected_rows=bronze_row_count,
        details=(
            f"Missing required columns={schema_val.missing_columns}; "
            f"unexpected columns={schema_val.unexpected_columns}"
        ),
    )
    return QualityReport(
        run_id=run_id,
        dataset_name=dataset_name,
        execution_mode=execution_mode,
        overall_passed=False,
        quality_score=0.0,
        total_bronze_rows=bronze_row_count,
        valid_silver_rows=0,
        quarantined_rows=0,
        duplicate_rows_removed=0,
        schema_valid=False,
        missing_columns=schema_val.missing_columns,
        unexpected_columns=schema_val.unexpected_columns,
        checks=[check],
        summary=(
            f"FAILED schema contract before Silver transform: "
            f"missing={schema_val.missing_columns}, unexpected={schema_val.unexpected_columns}."
        ),
    )
