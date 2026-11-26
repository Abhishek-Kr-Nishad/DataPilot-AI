"""Tests for AWS S3 storage backend (with botocore Stubber), Athena DDL, and Glue ETL script."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import boto3
import pytest
from botocore.stub import ANY, Stubber
from pyspark.sql import SparkSession

from datapilot.config import AWSDeploymentConfig, load_settings
from datapilot.data_generator import generate_ecommerce_dataset
from datapilot.metadata.models import LayerMetrics, PipelineRunRecord
from datapilot.storage.s3_storage import (
    AWSIntegrationError,
    S3StorageBackend,
    sync_run_artifacts_to_s3,
)


def _load_glue_module():
    glue_path = Path(__file__).resolve().parents[1] / "aws" / "glue_etl_job.py"
    spec = importlib.util.spec_from_file_location("glue_etl_job", glue_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_ddl_module():
    ddl_path = Path(__file__).resolve().parents[1] / "aws" / "glue_catalog_ddl.py"
    spec = importlib.util.spec_from_file_location("glue_catalog_ddl", ddl_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_s3_backend_requires_bucket() -> None:
    cfg = AWSDeploymentConfig(s3_bucket=None)
    with pytest.raises(AWSIntegrationError):
        S3StorageBackend.from_aws_config(cfg)


def test_s3_upload_and_sync_with_botocore_stubber(tmp_path: Path) -> None:
    s3_client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)

    raw_csv = tmp_path / "orders.csv"
    raw_csv.write_text("order_id,unit_price\nORD-1,10.0\n", encoding="utf-8")
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    (bronze_dir / "part-000.parquet").write_bytes(b"PAR1")

    stubber.add_response("head_bucket", {}, {"Bucket": "my-test-bucket"})
    stubber.add_response(
        "put_object",
        {},
        {"Bucket": "my-test-bucket", "Key": "datapilot-ai/raw/batch1/orders.csv", "Body": ANY},
    )
    stubber.add_response(
        "put_object",
        {},
        {
            "Bucket": "my-test-bucket",
            "Key": "datapilot-ai/bronze/batch1/part-000.parquet",
            "Body": ANY,
        },
    )

    backend = S3StorageBackend(
        bucket="my-test-bucket", prefix="datapilot-ai", s3_client=s3_client
    )
    settings = load_settings(project_root=tmp_path)
    run_record = PipelineRunRecord(
        run_id="run_aws_1",
        batch_id="batch1",
        status="SUCCESS",
        requested_mode="aws",
        effective_mode="aws",
        source_file=str(raw_csv),
        layers={
            "bronze": LayerMetrics(layer="bronze", path=str(bronze_dir), row_count=1, column_count=2)
        },
    )

    with stubber:
        synced = sync_run_artifacts_to_s3(settings, run_record, s3_backend=backend)
        stubber.assert_no_pending_responses()

    assert synced.aws_synced is True
    assert synced.layers["bronze"].s3_uri == "s3://my-test-bucket/datapilot-ai/bronze/batch1"


def test_athena_ddl_rendering_and_execution_stub() -> None:
    ddl_mod = _load_ddl_module()
    stmts = ddl_mod.render_athena_ddl_statements("datapilot_db", "s3://demo-bucket/datapilot-ai")
    assert len(stmts) == 5
    assert "s3://demo-bucket/datapilot-ai/silver/" in stmts[2]

    athena_client = boto3.client(
        "athena",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(athena_client)
    for i in range(5):
        stubber.add_response(
            "start_query_execution",
            {"QueryExecutionId": f"qid-{i}"},
            {
                "QueryString": ANY,
                "QueryExecutionContext": {"Database": "datapilot_db"},
                "ResultConfiguration": {"OutputLocation": "s3://demo-bucket/athena-results/"},
            },
        )

    cfg = AWSDeploymentConfig(
        region="us-east-1",
        s3_bucket="demo-bucket",
        s3_prefix="datapilot-ai",
        glue_database="datapilot_db",
        athena_output_location="s3://demo-bucket/athena-results/",
    )
    with stubber:
        qids = ddl_mod.execute_athena_ddl(cfg, athena_client=athena_client)
        stubber.assert_no_pending_responses()
    assert len(qids) == 5


def test_glue_etl_job_transform_compatibility(spark_session: SparkSession, tmp_path: Path) -> None:
    glue_mod = _load_glue_module()
    raw_csv = generate_ecommerce_dataset(tmp_path / "orders.csv", num_rows=40, seed=9, scenario="clean")

    summary = glue_mod.run_medallion_etl(
        spark=spark_session,
        input_uri=str(raw_csv),
        bronze_uri=str(tmp_path / "bronze"),
        silver_uri=str(tmp_path / "silver"),
        quarantine_uri=str(tmp_path / "quarantine"),
        gold_uri=str(tmp_path / "gold"),
        batch_id="glue-test-batch",
        execution_mode="local_glue_test",
    )
    assert summary["bronze_rows"] == 40
    assert summary["silver_rows"] == 40
    assert summary["quarantine_rows"] == 0
    assert summary["gold_daily_revenue_rows"] >= 1
