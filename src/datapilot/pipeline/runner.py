"""End-to-end Medallion pipeline orchestrator (Bronze -> Silver -> Quality -> Gold)."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from datapilot.config import AppSettings
from datapilot.logging_utils import get_logger
from datapilot.metadata.models import PipelineRunRecord, QualityReport, utc_now_iso
from datapilot.metadata.store import MetadataStore, create_metadata_store
from datapilot.pipeline.bronze import ingest_to_bronze
from datapilot.pipeline.gold import build_gold_aggregations
from datapilot.pipeline.quality import (
    build_schema_drift_quality_report,
    evaluate_data_quality,
)
from datapilot.pipeline.schemas import SchemaDriftError
from datapilot.pipeline.silver import transform_bronze_to_silver
from datapilot.spark_session import spark_session_context
from datapilot.storage.s3_storage import AWSIntegrationError, sync_run_artifacts_to_s3


@dataclass
class PipelineExecutionResult:
    """Complete result returned by a pipeline execution."""

    run_record: PipelineRunRecord
    quality_report: QualityReport | None


def _generate_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short_hex = uuid.uuid4().hex[:6]
    return f"run_{stamp}_{short_hex}"


def run_pipeline(
    settings: AppSettings,
    raw_csv_path: Path,
    scenario: str = "clean",
    batch_id: str | None = None,
    fail_on_quality_error: bool = True,
    sync_to_aws: bool = False,
    spark: SparkSession | None = None,
    metadata_store: MetadataStore | None = None,
) -> PipelineExecutionResult:
    """Execute Bronze -> Silver -> Quality -> Gold pipeline and persist manifests.

    Args:
        settings: Loaded application settings.
        raw_csv_path: Input CSV dataset path.
        scenario: Descriptive label of the dataset scenario.
        batch_id: Optional batch identifier; defaults to generated run_id.
        fail_on_quality_error: When True, halt before Gold if quality gate fails.
        sync_to_aws: Upload generated Bronze/Silver/Gold/Report artifacts to S3 when configured.
        spark: Optional pre-existing SparkSession (useful for test suites).
        metadata_store: Optional pre-configured MetadataStore.

    Returns:
        PipelineExecutionResult containing final run status and optional quality report.
    """
    settings.ensure_directories()
    run_id = _generate_run_id()
    effective_batch = batch_id or run_id
    log_file = settings.local.logs_dir / f"{run_id}.log"
    logger = get_logger("datapilot.pipeline", level=settings.local.log_level, log_file=log_file)
    store = metadata_store or create_metadata_store(settings)

    notes: list[str] = []
    if settings.fallback_reason:
        notes.append(settings.fallback_reason)
        logger.warning(settings.fallback_reason)

    record = PipelineRunRecord(
        run_id=run_id,
        batch_id=effective_batch,
        status="RUNNING",
        requested_mode=settings.requested_mode,
        effective_mode=settings.effective_mode,
        aws_synced=False,
        fallback_reason=settings.fallback_reason,
        source_file=str(raw_csv_path),
        scenario=scenario,
        notes=notes,
    )
    store.save_run(record)

    t_start = time.perf_counter()
    quality_report: QualityReport | None = None
    bronze_row_count = 0

    try:
        with spark_session_context(
            settings, app_name_suffix=run_id, existing_spark=spark
        ) as active_spark:
            bronze_res = ingest_to_bronze(
                spark=active_spark,
                raw_csv_path=raw_csv_path,
                bronze_dir=settings.local.bronze_dir,
                batch_id=effective_batch,
                execution_mode=settings.effective_mode,
            )
            record.layers["bronze"] = bronze_res.metrics
            bronze_row_count = bronze_res.row_count

            silver_res = transform_bronze_to_silver(
                spark=active_spark,
                bronze_df=bronze_res.dataframe,
                silver_dir=settings.local.silver_dir,
                quarantine_dir=settings.local.quarantine_dir,
                batch_id=effective_batch,
                execution_mode=settings.effective_mode,
            )
            record.layers["silver"] = silver_res.silver_metrics
            record.layers["quarantine"] = silver_res.quarantine_metrics

            quality_report = evaluate_data_quality(
                run_id=run_id,
                dataset_name=raw_csv_path.name,
                silver_result=silver_res,
                thresholds=settings.quality,
                execution_mode=settings.effective_mode,
            )
            quality_path = store.save_quality_report(quality_report)
            record.quality_report_path = quality_path

            if not quality_report.overall_passed and fail_on_quality_error:
                record.status = "QUALITY_FAILED"
                record.error_type = "DataQualityGateError"
                record.error_message = quality_report.summary
                record.notes.append(
                    "Gold aggregation skipped because blocking quality checks failed."
                )
                logger.warning("Pipeline halted at quality gate: %s", quality_report.summary)
            else:
                gold_res = build_gold_aggregations(
                    spark=active_spark,
                    silver_df=silver_res.silver_df,
                    gold_dir=settings.local.gold_dir,
                    batch_id=effective_batch,
                    execution_mode=settings.effective_mode,
                )
                record.layers.update(gold_res.metrics)
                record.status = "SUCCESS" if quality_report.overall_passed else "QUALITY_FAILED"
                logger.info("Pipeline run %s completed with status=%s", run_id, record.status)

    except SchemaDriftError as exc:
        logger.exception("Pipeline %s failed due to schema drift", run_id)
        quality_report = build_schema_drift_quality_report(
            run_id=run_id,
            dataset_name=raw_csv_path.name,
            schema_val=exc.validation,
            bronze_row_count=bronze_row_count,
            execution_mode=settings.effective_mode,
        )
        record.quality_report_path = store.save_quality_report(quality_report)
        record.status = "FAILED"
        record.error_type = "SchemaDriftError"
        record.error_message = str(exc)

    except Exception as exc:
        logger.exception("Pipeline %s failed with unexpected error", run_id)
        record.status = "FAILED"
        record.error_type = type(exc).__name__
        record.error_message = str(exc)

    finally:
        if sync_to_aws or settings.effective_mode == "aws":
            if settings.aws.is_ready_for_aws():
                try:
                    record = sync_run_artifacts_to_s3(settings, record)
                    logger.info("Synced run %s artifacts to AWS S3", run_id)
                except AWSIntegrationError as exc:
                    logger.warning("AWS S3 sync failed for run %s: %s", run_id, exc)
                    record.notes.append(f"AWS S3 sync failed: {exc}")
            else:
                msg = (
                    "AWS S3 sync skipped because AWS_S3_BUCKET or credentials are not configured."
                )
                logger.warning(msg)
                record.notes.append(msg)

        record.completed_at = utc_now_iso()
        record.duration_seconds = round(time.perf_counter() - t_start, 3)
        store.save_run(record)

    return PipelineExecutionResult(run_record=record, quality_report=quality_report)
