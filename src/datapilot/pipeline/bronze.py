"""Bronze layer ingestion: raw CSV preservation with lineage metadata."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datapilot.logging_utils import get_logger
from datapilot.metadata.models import ExecutionMode, LayerMetrics

logger = get_logger(__name__)


@dataclass
class BronzeResult:
    """Outcome of Bronze layer raw ingestion."""

    dataframe: DataFrame
    metrics: LayerMetrics
    raw_columns: list[str]
    row_count: int


def ingest_to_bronze(
    spark: SparkSession,
    raw_csv_path: Path,
    bronze_dir: Path,
    batch_id: str,
    execution_mode: ExecutionMode = "local",
) -> BronzeResult:
    """Ingest a raw CSV file into Bronze Parquet without lossy type coercion.

    Args:
        spark: Active SparkSession.
        raw_csv_path: Path to source CSV file.
        bronze_dir: Base directory for Bronze Parquet output.
        batch_id: Unique batch identifier for lineage tracking.
        execution_mode: Execution environment label ('local' or 'aws').

    Returns:
        BronzeResult containing the Bronze DataFrame and LayerMetrics.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw input CSV not found: {raw_csv_path}")

    logger.info("Reading raw CSV for Bronze ingestion: %s", raw_csv_path)
    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("escape", '"')
        .csv(str(raw_csv_path))
    )

    raw_columns = list(raw_df.columns)
    bronze_df = (
        raw_df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(raw_csv_path.name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_execution_mode", F.lit(execution_mode))
    )

    output_path = bronze_dir / batch_id
    output_path.mkdir(parents=True, exist_ok=True)
    bronze_df.write.mode("overwrite").parquet(str(output_path))

    persisted_df = spark.read.parquet(str(output_path))
    row_count = persisted_df.count()
    logger.info("Bronze layer materialized: path=%s rows=%d", output_path, row_count)

    metrics = LayerMetrics(
        layer="bronze",
        path=str(output_path),
        format="parquet",
        row_count=row_count,
        column_count=len(persisted_df.columns),
        columns=list(persisted_df.columns),
        execution_mode=execution_mode,
    )
    return BronzeResult(
        dataframe=persisted_df,
        metrics=metrics,
        raw_columns=raw_columns,
        row_count=row_count,
    )
