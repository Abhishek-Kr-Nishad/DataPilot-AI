"""Silver layer transformations: normalization, validation, deduplication, quarantine."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from datapilot.logging_utils import get_logger
from datapilot.metadata.models import ExecutionMode, LayerMetrics
from datapilot.pipeline.schemas import (
    CRITICAL_NOT_NULL_COLUMNS,
    SchemaDriftError,
    SchemaValidationResult,
    VALID_CURRENCIES,
    VALID_ORDER_STATUSES,
    validate_raw_columns,
)

logger = get_logger(__name__)


@dataclass
class SilverResult:
    """Outcome of Silver cleaning, validation, deduplication, and quarantine routing."""

    silver_df: DataFrame
    quarantine_df: DataFrame
    silver_metrics: LayerMetrics
    quarantine_metrics: LayerMetrics
    schema_validation: SchemaValidationResult
    bronze_row_count: int
    valid_before_dedup_count: int
    silver_row_count: int
    quarantined_row_count: int
    duplicate_rows_removed: int
    null_counts: dict[str, int]
    quarantine_reason_counts: dict[str, int]


def _blank_to_null(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(F.col(col_name).isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def _compute_bronze_null_counts(bronze_df: DataFrame) -> dict[str, int]:
    agg_exprs = [
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(c)
        for c in CRITICAL_NOT_NULL_COLUMNS
        if c in bronze_df.columns
    ]
    if not agg_exprs:
        return {}
    row = bronze_df.agg(*agg_exprs).collect()[0]
    return {col_name: int(row[col_name] or 0) for col_name in row.asDict()}


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_df: DataFrame,
    silver_dir: Path,
    quarantine_dir: Path,
    batch_id: str,
    execution_mode: ExecutionMode = "local",
) -> SilverResult:
    """Clean, normalize, validate, and deduplicate Bronze data into Silver + Quarantine.

    Raises:
        SchemaDriftError: When required columns from the raw contract are missing.
    """
    schema_validation = validate_raw_columns(list(bronze_df.columns))
    if schema_validation.missing_columns:
        logger.error(
            "Schema drift blocked Silver processing: missing=%s unexpected=%s",
            schema_validation.missing_columns,
            schema_validation.unexpected_columns,
        )
        raise SchemaDriftError(schema_validation)

    bronze_row_count = bronze_df.count()
    null_counts = _compute_bronze_null_counts(bronze_df) if bronze_row_count > 0 else {}

    normalized = (
        bronze_df.withColumn("order_id_clean", _blank_to_null("order_id"))
        .withColumn("customer_id_clean", _blank_to_null("customer_id"))
        .withColumn("product_id_clean", _blank_to_null("product_id"))
        .withColumn("product_name_clean", _blank_to_null("product_name"))
        .withColumn("category_clean", F.initcap(_blank_to_null("category")))
        .withColumn("currency_clean", F.upper(_blank_to_null("currency")))
        .withColumn("payment_method_clean", F.upper(_blank_to_null("payment_method")))
        .withColumn("order_status_clean", F.upper(_blank_to_null("order_status")))
        .withColumn("country_clean", F.upper(_blank_to_null("country")))
        .withColumn("region_clean", _blank_to_null("region"))
        .withColumn(
            "order_ts_parsed",
            F.coalesce(
                F.expr("try_to_timestamp(trim(order_timestamp), 'yyyy-MM-dd HH:mm:ss')"),
                F.expr("try_to_timestamp(trim(order_timestamp))"),
            ),
        )
        .withColumn("quantity_parsed", F.expr("try_cast(trim(quantity) as int)"))
        .withColumn("unit_price_parsed", F.expr("try_cast(trim(unit_price) as double)"))
        .withColumn(
            "discount_pct_parsed",
            F.coalesce(F.expr("try_cast(trim(discount_pct) as double)"), F.lit(0.0)),
        )
    )

    reason_array = F.array_compact(
        F.array(
            F.when(F.col("order_id_clean").isNull(), F.lit("missing_order_id")),
            F.when(F.col("customer_id_clean").isNull(), F.lit("missing_customer_id")),
            F.when(F.col("product_id_clean").isNull(), F.lit("missing_product_id")),
            F.when(F.col("order_ts_parsed").isNull(), F.lit("invalid_or_missing_timestamp")),
            F.when(
                F.col("quantity_parsed").isNull() | (F.col("quantity_parsed") <= 0),
                F.lit("invalid_quantity"),
            ),
            F.when(
                F.col("unit_price_parsed").isNull() | (F.col("unit_price_parsed") <= 0.0),
                F.lit("invalid_unit_price"),
            ),
            F.when(
                (F.col("discount_pct_parsed") < 0.0) | (F.col("discount_pct_parsed") > 100.0),
                F.lit("invalid_discount_pct"),
            ),
            F.when(
                F.col("order_status_clean").isNull()
                | (~F.col("order_status_clean").isin(sorted(VALID_ORDER_STATUSES))),
                F.lit("invalid_order_status"),
            ),
            F.when(
                F.col("currency_clean").isNull()
                | (~F.col("currency_clean").isin(sorted(VALID_CURRENCIES))),
                F.lit("invalid_currency"),
            ),
        )
    )

    evaluated = normalized.withColumn("_quarantine_reasons", reason_array).withColumn(
        "_is_valid", F.size(F.col("_quarantine_reasons")) == 0
    )

    quarantine_df = evaluated.filter(~F.col("_is_valid")).select(
        "order_id",
        "customer_id",
        "order_timestamp",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "discount_pct",
        "currency",
        "order_status",
        "country",
        "region",
        "_batch_id",
        "_source_file",
        "_ingested_at",
        F.concat_ws("; ", F.col("_quarantine_reasons")).alias("_quarantine_reason"),
    )

    valid_df = evaluated.filter(F.col("_is_valid"))
    valid_before_dedup = valid_df.count()

    dedup_window = Window.partitionBy("order_id_clean").orderBy(
        F.col("order_ts_parsed").desc(),
        F.col("_ingested_at").desc(),
    )
    deduped_df = (
        valid_df.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    silver_projected = deduped_df.select(
        F.col("order_id_clean").alias("order_id"),
        F.col("customer_id_clean").alias("customer_id"),
        F.col("order_ts_parsed").alias("order_timestamp"),
        F.to_date(F.col("order_ts_parsed")).alias("order_date"),
        F.col("product_id_clean").alias("product_id"),
        F.coalesce(F.col("product_name_clean"), F.lit("Unknown Product")).alias("product_name"),
        F.coalesce(F.col("category_clean"), F.lit("Uncategorized")).alias("category"),
        F.col("quantity_parsed").alias("quantity"),
        F.round(F.col("unit_price_parsed"), 2).alias("unit_price"),
        F.round(F.col("discount_pct_parsed"), 2).alias("discount_pct"),
        F.round(F.col("quantity_parsed") * F.col("unit_price_parsed"), 2).alias("gross_amount"),
        F.round(
            F.col("quantity_parsed")
            * F.col("unit_price_parsed")
            * (F.col("discount_pct_parsed") / 100.0),
            2,
        ).alias("discount_amount"),
        F.round(
            (F.col("quantity_parsed") * F.col("unit_price_parsed"))
            * (1.0 - (F.col("discount_pct_parsed") / 100.0)),
            2,
        ).alias("net_amount"),
        F.col("currency_clean").alias("currency"),
        F.coalesce(F.col("payment_method_clean"), F.lit("UNKNOWN")).alias("payment_method"),
        F.col("order_status_clean").alias("order_status"),
        F.coalesce(F.col("country_clean"), F.lit("UNKNOWN")).alias("country"),
        F.coalesce(F.col("region_clean"), F.lit("UNKNOWN")).alias("region"),
        F.col("_batch_id"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_cleaned_at"),
    )

    silver_path = silver_dir / batch_id
    quarantine_path = quarantine_dir / batch_id
    silver_path.mkdir(parents=True, exist_ok=True)
    quarantine_path.mkdir(parents=True, exist_ok=True)

    silver_projected.write.mode("overwrite").parquet(str(silver_path))
    quarantine_df.write.mode("overwrite").parquet(str(quarantine_path))

    persisted_silver = spark.read.parquet(str(silver_path))
    persisted_quarantine = spark.read.parquet(str(quarantine_path))

    silver_count = persisted_silver.count()
    quarantine_count = persisted_quarantine.count()
    duplicate_rows_removed = max(0, valid_before_dedup - silver_count)

    quarantine_reason_counts: dict[str, int] = {}
    if quarantine_count > 0:
        reason_rows = (
            persisted_quarantine.groupBy("_quarantine_reason")
            .count()
            .orderBy(F.col("count").desc())
            .limit(20)
            .collect()
        )
        quarantine_reason_counts = {
            str(r["_quarantine_reason"] or "unknown"): int(r["count"]) for r in reason_rows
        }

    logger.info(
        "Silver transform complete: bronze=%d silver=%d quarantined=%d duplicates_removed=%d",
        bronze_row_count,
        silver_count,
        quarantine_count,
        duplicate_rows_removed,
    )

    silver_metrics = LayerMetrics(
        layer="silver",
        path=str(silver_path),
        format="parquet",
        row_count=silver_count,
        column_count=len(persisted_silver.columns),
        columns=list(persisted_silver.columns),
        execution_mode=execution_mode,
    )
    quarantine_metrics = LayerMetrics(
        layer="quarantine",
        path=str(quarantine_path),
        format="parquet",
        row_count=quarantine_count,
        column_count=len(persisted_quarantine.columns),
        columns=list(persisted_quarantine.columns),
        execution_mode=execution_mode,
    )

    return SilverResult(
        silver_df=persisted_silver,
        quarantine_df=persisted_quarantine,
        silver_metrics=silver_metrics,
        quarantine_metrics=quarantine_metrics,
        schema_validation=schema_validation,
        bronze_row_count=bronze_row_count,
        valid_before_dedup_count=valid_before_dedup,
        silver_row_count=silver_count,
        quarantined_row_count=quarantine_count,
        duplicate_rows_removed=duplicate_rows_removed,
        null_counts=null_counts,
        quarantine_reason_counts=quarantine_reason_counts,
    )
