"""Gold layer analytical aggregations built from validated Silver data."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datapilot.logging_utils import get_logger
from datapilot.metadata.models import ExecutionMode, LayerMetrics

logger = get_logger(__name__)


@dataclass
class GoldResult:
    """Materialized Gold analytical tables and their metrics."""

    tables: dict[str, DataFrame]
    metrics: dict[str, LayerMetrics]


def build_gold_aggregations(
    spark: SparkSession,
    silver_df: DataFrame,
    gold_dir: Path,
    batch_id: str,
    execution_mode: ExecutionMode = "local",
) -> GoldResult:
    """Compute analytical Gold aggregations and persist them as Parquet."""
    active_orders = silver_df.filter(~F.col("order_status").isin("CANCELLED", "RETURNED"))

    daily_revenue_df = (
        silver_df.groupBy("order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("order_status") == "COMPLETED", 1).otherwise(0)).alias(
                "completed_orders"
            ),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("total_discounts"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value"),
        )
        .orderBy("order_date")
    )

    category_perf_df = (
        silver_df.groupBy("category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.sum(
                F.when(F.col("order_status").isin("CANCELLED", "RETURNED"), 1).otherwise(0)
            ).alias("return_or_cancel_count"),
        )
        .orderBy(F.col("net_revenue").desc())
    )

    regional_sales_df = (
        active_orders.groupBy("region", "country", "currency")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.avg("net_amount"), 2).alias("avg_order_value"),
        )
        .orderBy(F.col("net_revenue").desc())
    )

    customer_ltv_df = (
        active_orders.groupBy("customer_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("net_amount"), 2).alias("lifetime_net_spend"),
            F.min("order_timestamp").alias("first_order_at"),
            F.max("order_timestamp").alias("last_order_at"),
        )
        .orderBy(F.col("lifetime_net_spend").desc())
    )

    definitions = {
        "daily_revenue": daily_revenue_df,
        "category_performance": category_perf_df,
        "regional_sales": regional_sales_df,
        "customer_ltv_summary": customer_ltv_df,
    }

    persisted_tables: dict[str, DataFrame] = {}
    metrics_map: dict[str, LayerMetrics] = {}

    batch_gold_dir = gold_dir / batch_id
    batch_gold_dir.mkdir(parents=True, exist_ok=True)

    for table_name, df in definitions.items():
        table_path = batch_gold_dir / table_name
        df.write.mode("overwrite").parquet(str(table_path))
        persisted = spark.read.parquet(str(table_path))
        row_count = persisted.count()

        # Materialize JSON summary sidecar alongside the Parquet folder so FastAPI
        # analytics endpoints respond instantaneously even if PyArrow <20 cannot parse
        # Spark 4 Parquet size statistics.
        preview_rows = []
        for row in persisted.limit(500).collect():
            item = {}
            for k, v in row.asDict().items():
                if isinstance(v, (dt.date, dt.datetime)):
                    item[k] = v.isoformat()
                else:
                    item[k] = v
            preview_rows.append(item)
        sidecar_path = batch_gold_dir / f"_{table_name}_summary.json"
        sidecar_path.write_text(json.dumps(preview_rows), encoding="utf-8")

        persisted_tables[table_name] = persisted
        metrics_map[f"gold_{table_name}"] = LayerMetrics(
            layer=f"gold.{table_name}",
            path=str(table_path),
            format="parquet",
            row_count=row_count,
            column_count=len(persisted.columns),
            columns=list(persisted.columns),
            execution_mode=execution_mode,
        )
        logger.info("Gold table materialized: %s rows=%d path=%s", table_name, row_count, table_path)

    return GoldResult(tables=persisted_tables, metrics=metrics_map)
