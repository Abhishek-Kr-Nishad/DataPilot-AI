"""AWS Glue 4.0 compatible PySpark ETL job for DataPilot AI.

Can execute inside AWS Glue (via awsglue.context.GlueContext and Job) or be
imported directly in local unit tests using a standard SparkSession. Never claims
local execution is an AWS Glue deployment.
"""

from __future__ import annotations

import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

VALID_ORDER_STATUSES = ("COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "RETURNED")
VALID_CURRENCIES = ("USD", "EUR", "GBP", "INR", "CAD", "SGD")
REQUIRED_RAW_COLUMNS = (
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
    "payment_method",
    "order_status",
    "country",
    "region",
)


def _blank_to_null(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(F.col(col_name).isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def validate_contract(columns: list[str]) -> list[str]:
    """Return missing required columns from an ingested CSV/Bronze DataFrame."""
    col_set = set(columns)
    return [col_name for col_name in REQUIRED_RAW_COLUMNS if col_name not in col_set]


def run_medallion_etl(
    spark: SparkSession,
    input_uri: str,
    bronze_uri: str,
    silver_uri: str,
    quarantine_uri: str,
    gold_uri: str,
    batch_id: str,
    execution_mode: str = "aws_glue",
) -> dict[str, Any]:
    """Execute Bronze -> Silver -> Gold Medallion ETL across URIs (S3 or local)."""
    spark.conf.set("spark.sql.ansi.enabled", "false")

    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(input_uri)
    )
    missing_cols = validate_contract(list(raw_df.columns))
    if missing_cols:
        raise ValueError(f"Schema drift detected in {input_uri}: missing columns {missing_cols}")

    bronze_df = (
        raw_df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(input_uri))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_execution_mode", F.lit(execution_mode))
    )
    bronze_df.write.mode("overwrite").parquet(bronze_uri)
    bronze_count = bronze_df.count()

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

    reasons = F.array_compact(
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
                | (~F.col("order_status_clean").isin(*VALID_ORDER_STATUSES)),
                F.lit("invalid_order_status"),
            ),
            F.when(
                F.col("currency_clean").isNull()
                | (~F.col("currency_clean").isin(*VALID_CURRENCIES)),
                F.lit("invalid_currency"),
            ),
        )
    )

    evaluated = normalized.withColumn("_quarantine_reasons", reasons).withColumn(
        "_is_valid", F.size(F.col("_quarantine_reasons")) == 0
    )

    quarantine_df = evaluated.filter(~F.col("_is_valid")).select(
        "order_id",
        "customer_id",
        "order_timestamp",
        "product_id",
        "quantity",
        "unit_price",
        "order_status",
        "_batch_id",
        "_ingested_at",
        F.concat_ws("; ", F.col("_quarantine_reasons")).alias("_quarantine_reason"),
    )
    quarantine_df.write.mode("overwrite").parquet(quarantine_uri)
    quarantine_count = quarantine_df.count()

    valid_df = evaluated.filter(F.col("_is_valid"))
    dedup_window = Window.partitionBy("order_id_clean").orderBy(
        F.col("order_ts_parsed").desc(),
        F.col("_ingested_at").desc(),
    )
    silver_df: DataFrame = (
        valid_df.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(
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
    )
    silver_df.write.mode("overwrite").parquet(silver_uri)
    silver_count = silver_df.count()

    daily_revenue = (
        silver_df.groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        )
        .orderBy("order_date")
    )
    daily_revenue.write.mode("overwrite").parquet(f"{gold_uri.rstrip('/')}/daily_revenue")

    category_perf = (
        silver_df.groupBy("category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        )
        .orderBy(F.col("net_revenue").desc())
    )
    category_perf.write.mode("overwrite").parquet(f"{gold_uri.rstrip('/')}/category_performance")

    return {
        "batch_id": batch_id,
        "execution_mode": execution_mode,
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "quarantine_rows": quarantine_count,
        "gold_daily_revenue_rows": daily_revenue.count(),
        "gold_category_rows": category_perf.count(),
    }


def main() -> None:
    """AWS Glue runtime entrypoint using GlueContext and Job."""
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_INPUT_URI",
            "S3_BRONZE_URI",
            "S3_SILVER_URI",
            "S3_QUARANTINE_URI",
            "S3_GOLD_URI",
            "BATCH_ID",
        ],
    )
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    run_medallion_etl(
        spark=spark,
        input_uri=args["S3_INPUT_URI"],
        bronze_uri=args["S3_BRONZE_URI"],
        silver_uri=args["S3_SILVER_URI"],
        quarantine_uri=args["S3_QUARANTINE_URI"],
        gold_uri=args["S3_GOLD_URI"],
        batch_id=args["BATCH_ID"],
        execution_mode="aws_glue",
    )
    job.commit()


if __name__ == "__main__":
    main()
