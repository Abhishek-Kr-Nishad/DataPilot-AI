"""Generate and optionally execute Amazon Athena / AWS Glue Data Catalog DDL."""

from __future__ import annotations

from typing import Any

import boto3

from datapilot.config import AWSDeploymentConfig


def render_athena_ddl_statements(database: str, s3_base_uri: str) -> list[str]:
    """Render parameterized Amazon Athena external table DDLs for all Medallion layers."""
    base = s3_base_uri.rstrip("/")
    return [
        f"CREATE DATABASE IF NOT EXISTS {database};",
        (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.bronze_orders (\n"
            "  order_id string,\n"
            "  customer_id string,\n"
            "  order_timestamp string,\n"
            "  product_id string,\n"
            "  product_name string,\n"
            "  category string,\n"
            "  quantity string,\n"
            "  unit_price string,\n"
            "  discount_pct string,\n"
            "  currency string,\n"
            "  payment_method string,\n"
            "  order_status string,\n"
            "  country string,\n"
            "  region string,\n"
            "  _batch_id string,\n"
            "  _source_file string,\n"
            "  _ingested_at timestamp,\n"
            "  _execution_mode string\n"
            ")\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{base}/bronze/';"
        ),
        (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.silver_orders (\n"
            "  order_id string,\n"
            "  customer_id string,\n"
            "  order_timestamp timestamp,\n"
            "  order_date date,\n"
            "  product_id string,\n"
            "  product_name string,\n"
            "  category string,\n"
            "  quantity int,\n"
            "  unit_price double,\n"
            "  discount_pct double,\n"
            "  gross_amount double,\n"
            "  discount_amount double,\n"
            "  net_amount double,\n"
            "  currency string,\n"
            "  payment_method string,\n"
            "  order_status string,\n"
            "  country string,\n"
            "  region string,\n"
            "  _batch_id string,\n"
            "  _source_file string,\n"
            "  _ingested_at timestamp,\n"
            "  _cleaned_at timestamp\n"
            ")\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{base}/silver/';"
        ),
        (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.silver_quarantine (\n"
            "  order_id string,\n"
            "  customer_id string,\n"
            "  order_timestamp string,\n"
            "  product_id string,\n"
            "  quantity string,\n"
            "  unit_price string,\n"
            "  order_status string,\n"
            "  _batch_id string,\n"
            "  _source_file string,\n"
            "  _ingested_at timestamp,\n"
            "  _quarantine_reason string\n"
            ")\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{base}/quarantine/';"
        ),
        (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.gold_daily_revenue (\n"
            "  order_date date,\n"
            "  total_orders bigint,\n"
            "  completed_orders bigint,\n"
            "  total_units bigint,\n"
            "  gross_revenue double,\n"
            "  total_discounts double,\n"
            "  net_revenue double,\n"
            "  avg_order_value double\n"
            ")\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{base}/gold/daily_revenue/';"
        ),
    ]


def execute_athena_ddl(
    aws_config: AWSDeploymentConfig,
    athena_client: Any | None = None,
) -> list[str]:
    """Submit rendered Athena DDL statements to Amazon Athena (requires S3 output URI)."""
    if not aws_config.is_bucket_configured() or not aws_config.athena_output_location:
        raise ValueError(
            "Both AWS_S3_BUCKET and AWS_ATHENA_OUTPUT_LOCATION must be set to execute Athena DDL."
        )

    client = athena_client or boto3.client("athena", region_name=aws_config.region)
    s3_base = f"s3://{aws_config.s3_bucket}/{aws_config.s3_prefix.strip('/')}"
    statements = render_athena_ddl_statements(aws_config.glue_database, s3_base)
    execution_ids: list[str] = []

    for stmt in statements:
        resp = client.start_query_execution(
            QueryString=stmt,
            QueryExecutionContext={"Database": aws_config.glue_database},
            ResultConfiguration={"OutputLocation": aws_config.athena_output_location},
        )
        execution_ids.append(resp["QueryExecutionId"])

    return execution_ids
