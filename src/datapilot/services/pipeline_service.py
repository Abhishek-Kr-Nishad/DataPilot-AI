"""Pipeline execution, status, and Medallion layer inspection service."""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession

from datapilot.core.config import AppSettings
from datapilot.data_generator import ScenarioName, generate_ecommerce_dataset
from datapilot.metadata.store import MetadataStore
from datapilot.pipeline.runner import PipelineExecutionResult, run_pipeline
from datapilot.schemas.api import (
    LayerPreviewResponse,
    PipelineRunResponse,
    PipelineStatusResponse,
)
from datapilot.schemas.domain import PipelineRunRecord, QualityReport
from datapilot.spark_session import spark_session_context


def dataframe_to_json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a Pandas DataFrame into JSON-serializable dictionaries."""
    if df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
    records: list[dict[str, Any]] = []
    for raw_row in out.to_dict(orient="records"):
        clean_row: dict[str, Any] = {}
        for key, val in raw_row.items():
            if pd.isna(val):
                clean_row[key] = None
            elif isinstance(val, (dt.date, dt.datetime)):
                clean_row[key] = val.isoformat()
            else:
                clean_row[key] = val
        records.append(clean_row)
    return records


def read_parquet_safely(
    layer_path: Path,
    settings: AppSettings,
    spark: SparkSession | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Read Parquet directory via PyArrow/Pandas with Spark fallback for Spark 4 size stats."""
    if not layer_path.exists():
        return pd.DataFrame()
    if layer_path.is_dir() and not any(layer_path.glob("*.parquet")):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(layer_path)
        return df.head(limit) if limit is not None else df
    except (OSError, ValueError):
        with spark_session_context(
            settings,
            app_name_suffix="parquet-preview",
            existing_spark=spark,
        ) as active_spark:
            sdf = active_spark.read.parquet(str(layer_path))
            if limit is not None:
                sdf = sdf.limit(limit)
            return sdf.toPandas()


class PipelineService:
    """Encapsulates ETL orchestration, run manifests, quality reports, and layer previews."""

    def __init__(
        self,
        settings: AppSettings,
        metadata_store: MetadataStore,
        spark: SparkSession | None = None,
    ) -> None:
        self.settings = settings
        self.metadata_store = metadata_store
        self.spark = spark

    def run_synthetic_batch(
        self,
        scenario: ScenarioName = "clean",
        num_rows: int = 250,
        seed: int = 42,
        fail_on_quality_error: bool = True,
        sync_to_aws: bool = False,
    ) -> PipelineRunResponse:
        """Generate deterministic synthetic CSV data and run the Medallion ETL pipeline."""
        csv_path = (
            self.settings.local.raw_dir
            / f"orders_{scenario}_seed{seed}_n{num_rows}.csv"
        )
        generate_ecommerce_dataset(
            output_path=csv_path,
            num_rows=num_rows,
            seed=seed,
            scenario=scenario,
        )
        result: PipelineExecutionResult = run_pipeline(
            settings=self.settings,
            raw_csv_path=csv_path,
            scenario=scenario,
            fail_on_quality_error=fail_on_quality_error,
            sync_to_aws=sync_to_aws,
            spark=self.spark,
            metadata_store=self.metadata_store,
        )
        return PipelineRunResponse(
            run_record=result.run_record,
            quality_report=result.quality_report,
        )

    def run_uploaded_csv(
        self,
        filename: str,
        content: bytes,
        fail_on_quality_error: bool = True,
        sync_to_aws: bool = False,
    ) -> PipelineRunResponse:
        """Save an uploaded raw CSV file and execute the Medallion ETL pipeline."""
        raw_name = Path(filename or "uploaded.csv").name
        if not raw_name.lower().endswith(".csv"):
            raise ValueError("Only .csv files are supported for raw ingestion.")
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", raw_name)
        target_csv = self.settings.local.raw_dir / safe_name
        target_csv.parent.mkdir(parents=True, exist_ok=True)
        target_csv.write_bytes(content)

        result = run_pipeline(
            settings=self.settings,
            raw_csv_path=target_csv,
            scenario="uploaded_csv",
            fail_on_quality_error=fail_on_quality_error,
            sync_to_aws=sync_to_aws,
            spark=self.spark,
            metadata_store=self.metadata_store,
        )
        return PipelineRunResponse(
            run_record=result.run_record,
            quality_report=result.quality_report,
        )

    def list_runs(
        self,
        limit: int = 50,
        status_filter: str | None = None,
    ) -> list[PipelineRunRecord]:
        """List recent pipeline runs optionally filtered by status."""
        runs = self.metadata_store.list_runs(limit=limit)
        if status_filter:
            target = status_filter.strip().upper()
            runs = [r for r in runs if r.status == target]
        return runs

    def get_run(self, run_id: str) -> PipelineRunRecord | None:
        """Fetch a single pipeline run manifest."""
        return self.metadata_store.get_run(run_id)

    def get_status_overview(self, run_id: str | None = None) -> PipelineStatusResponse:
        """Return summary counts and latest/selected run status."""
        runs = self.metadata_store.list_runs(limit=200)
        target_run = self.metadata_store.get_run(run_id) if run_id else (runs[0] if runs else None)
        return PipelineStatusResponse(
            has_runs=len(runs) > 0,
            latest_run=target_run,
            total_runs=len(runs),
            success_count=sum(1 for r in runs if r.status == "SUCCESS"),
            quality_failed_count=sum(1 for r in runs if r.status == "QUALITY_FAILED"),
            failed_count=sum(1 for r in runs if r.status == "FAILED"),
        )

    def get_quality_report(self, run_id: str | None = None) -> QualityReport | None:
        """Fetch the quality report for run_id or the latest run with a report."""
        if run_id is not None:
            return self.metadata_store.get_quality_report(run_id)
        for run in self.metadata_store.list_runs(limit=50):
            report = self.metadata_store.get_quality_report(run.run_id)
            if report is not None:
                return report
        return None

    def preview_layer(
        self,
        run_id: str,
        layer: str,
        limit: int = 100,
    ) -> LayerPreviewResponse:
        """Return bounded JSON-safe preview records from a materialized Parquet layer."""
        record = self.metadata_store.get_run(run_id)
        if record is None:
            raise FileNotFoundError(f"Pipeline run not found: {run_id}")
        if layer not in record.layers:
            raise KeyError(
                f"Layer {layer!r} not found for run {run_id}. Available: {list(record.layers)}"
            )
        layer_meta = record.layers[layer]
        layer_path = Path(layer_meta.path)
        if layer_meta.layer.startswith("gold."):
            table_name = layer_meta.layer.split(".", 1)[1]
            sidecar = layer_path.parent / f"_{table_name}_summary.json"
            if sidecar.exists():
                rows = json.loads(sidecar.read_text(encoding="utf-8"))[:limit]
                cols = list(rows[0].keys()) if rows else layer_meta.columns
                return LayerPreviewResponse(
                    run_id=run_id,
                    layer=layer,
                    total_rows=layer_meta.row_count,
                    returned_rows=len(rows),
                    columns=cols,
                    records=rows,
                )

        df = read_parquet_safely(
            layer_path,
            settings=self.settings,
            spark=self.spark,
            limit=limit,
        )
        records = dataframe_to_json_records(df)
        return LayerPreviewResponse(
            run_id=run_id,
            layer=layer,
            total_rows=layer_meta.row_count,
            returned_rows=len(records),
            columns=list(df.columns) if not df.empty else layer_meta.columns,
            records=records,
        )
