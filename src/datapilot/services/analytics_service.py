"""Analytics summary service aggregating Gold and Quality metrics."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from datapilot.core.config import AppSettings
from datapilot.core.logging import get_logger
from datapilot.metadata.store import MetadataStore
from datapilot.schemas.api import AnalyticsSummaryResponse
from datapilot.schemas.domain import PipelineRunRecord
from datapilot.services.pipeline_service import dataframe_to_json_records, read_parquet_safely
from datapilot.spark_session import spark_session_context

logger = get_logger("datapilot.services.analytics")


class AnalyticsService:
    """Builds business and operational analytics summaries from Gold Parquet tables."""

    def __init__(
        self,
        settings: AppSettings,
        metadata_store: MetadataStore,
        spark: SparkSession | None = None,
    ) -> None:
        self.settings = settings
        self.metadata_store = metadata_store
        self.spark = spark

    def _select_target_run(self, run_id: str | None) -> PipelineRunRecord | None:
        if run_id is not None:
            return self.metadata_store.get_run(run_id)
        runs = self.metadata_store.list_runs(limit=50)
        for run in runs:
            if "gold_daily_revenue" in run.layers:
                return run
        return runs[0] if runs else None

    def get_summary(self, run_id: str | None = None) -> AnalyticsSummaryResponse:
        """Aggregate revenue, category, regional, customer LTV, and quality metrics."""
        target = self._select_target_run(run_id)
        if target is None:
            return AnalyticsSummaryResponse(
                execution_mode=self.settings.effective_mode,
                pipeline_status="NO_RUNS",
            )

        report = self.metadata_store.get_quality_report(target.run_id)
        bronze_rows = (
            report.total_bronze_rows
            if report is not None
            else (target.layers["bronze"].row_count if "bronze" in target.layers else 0)
        )
        valid_rows = (
            report.valid_silver_rows
            if report is not None
            else (target.layers["silver"].row_count if "silver" in target.layers else 0)
        )
        rejected_rows = (
            report.quarantined_rows
            if report is not None
            else (target.layers["quarantine"].row_count if "quarantine" in target.layers else 0)
        )
        duplicate_rows = report.duplicate_rows_removed if report is not None else 0
        quality_score = report.quality_score if report is not None else 0.0

        def _read_gold(key: str, table_name: str, limit: int = 100):
            layer = target.layers.get(key)
            if layer is None:
                return []
            layer_path = Path(layer.path)
            sidecar = layer_path.parent / f"_{table_name}_summary.json"
            if sidecar.exists():
                rows = json.loads(sidecar.read_text(encoding="utf-8"))
                return rows[:limit]
            try:
                df = read_parquet_safely(
                    layer_path,
                    settings=self.settings,
                    spark=self.spark,
                    limit=limit,
                )
                return dataframe_to_json_records(df)
            except Exception:
                return []

        gold_specs = [
            ("gold_daily_revenue", "daily_revenue", 180),
            ("gold_category_performance", "category_performance", 50),
            ("gold_regional_sales", "regional_sales", 50),
            ("gold_customer_ltv_summary", "customer_ltv_summary", 20),
        ]
        # Fast-path via JSON sidecars or PyArrow; if any table lacks a sidecar and PyArrow
        # fails, read missing ones within a single shared SparkSession.
        loaded_gold: dict[str, list] = {}
        missing_for_spark: list[tuple[str, Path, int]] = []
        for key, tname, lim in gold_specs:
            layer = target.layers.get(key)
            if layer is None:
                loaded_gold[key] = []
                continue
            lpath = Path(layer.path)
            sidecar = lpath.parent / f"_{tname}_summary.json"
            if sidecar.exists():
                try:
                    loaded_gold[key] = json.loads(sidecar.read_text(encoding="utf-8"))[:lim]
                    continue
                except Exception as exc:
                    logger.warning("Failed reading Gold sidecar %s: %s", sidecar, exc)

            if not lpath.exists() or (lpath.is_dir() and not any(lpath.glob("*.parquet"))):
                loaded_gold[key] = []
                continue

            try:
                import pandas as pd

                df = pd.read_parquet(lpath).head(lim)
                loaded_gold[key] = dataframe_to_json_records(df)
            except (OSError, ValueError):
                missing_for_spark.append((key, lpath, lim))
            except Exception as exc:
                logger.warning("Unexpected error reading Gold Parquet %s: %s", lpath, exc)
                loaded_gold[key] = []

        if missing_for_spark:
            try:
                with spark_session_context(
                    self.settings,
                    app_name_suffix="analytics-summary",
                    existing_spark=self.spark,
                ) as active_spark:
                    for key, lpath, lim in missing_for_spark:
                        df = active_spark.read.parquet(str(lpath)).limit(lim).toPandas()
                        loaded_gold[key] = dataframe_to_json_records(df)
            except Exception as exc:
                logger.exception("Spark fallback failed while reading Gold analytics: %s", exc)
                for key, _, _ in missing_for_spark:
                    loaded_gold.setdefault(key, [])

        daily_rev = loaded_gold.get("gold_daily_revenue", [])
        category_perf = loaded_gold.get("gold_category_performance", [])
        regional_sales = loaded_gold.get("gold_regional_sales", [])
        top_customers = loaded_gold.get("gold_customer_ltv_summary", [])

        total_gross = round(sum(float(r.get("gross_revenue") or 0.0) for r in daily_rev), 2)
        total_net = round(sum(float(r.get("net_revenue") or 0.0) for r in daily_rev), 2)
        total_discounts = round(sum(float(r.get("total_discounts") or 0.0) for r in daily_rev), 2)
        total_gold_orders = sum(int(r.get("total_orders") or 0) for r in daily_rev)
        avg_order_value = round(total_net / total_gold_orders, 2) if total_gold_orders > 0 else 0.0

        return AnalyticsSummaryResponse(
            run_id=target.run_id,
            has_data=len(daily_rev) > 0,
            execution_mode=target.effective_mode,
            pipeline_status=target.status,
            total_records_processed=bronze_rows,
            valid_records=valid_rows,
            rejected_records=rejected_rows,
            duplicate_records=duplicate_rows,
            data_quality_score=quality_score,
            total_gross_revenue=total_gross,
            total_net_revenue=total_net,
            total_discounts=total_discounts,
            avg_order_value=avg_order_value,
            daily_revenue=daily_rev,
            category_performance=category_perf,
            regional_sales=regional_sales,
            top_customers=top_customers,
        )
