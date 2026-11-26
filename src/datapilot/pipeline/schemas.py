"""Schema contracts and validation helpers for Medallion pipeline layers."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from datapilot.data_generator import EXPECTED_RAW_COLUMNS

RAW_REQUIRED_COLUMNS: list[str] = list(EXPECTED_RAW_COLUMNS)

CRITICAL_NOT_NULL_COLUMNS: list[str] = [
    "order_id",
    "customer_id",
    "order_timestamp",
    "product_id",
    "quantity",
    "unit_price",
]

VALID_ORDER_STATUSES: set[str] = {
    "COMPLETED",
    "SHIPPED",
    "PROCESSING",
    "CANCELLED",
    "RETURNED",
}

VALID_CURRENCIES: set[str] = {"USD", "EUR", "GBP", "INR", "CAD", "SGD"}

SILVER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("order_date", DateType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_pct", DoubleType(), False),
        StructField("gross_amount", DoubleType(), False),
        StructField("discount_amount", DoubleType(), False),
        StructField("net_amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("country", StringType(), False),
        StructField("region", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("_source_file", StringType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_cleaned_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class SchemaValidationResult:
    """Comparison between observed Bronze columns and expected contract."""

    schema_valid: bool
    missing_columns: list[str]
    unexpected_columns: list[str]
    observed_columns: list[str]


class SchemaDriftError(ValueError):
    """Raised when required raw columns are missing from the ingested dataset."""

    def __init__(self, validation: SchemaValidationResult) -> None:
        self.validation = validation
        super().__init__(
            f"Schema drift detected: missing={validation.missing_columns}, "
            f"unexpected={validation.unexpected_columns}"
        )


def validate_raw_columns(observed_columns: list[str]) -> SchemaValidationResult:
    """Validate observed CSV/Bronze columns against EXPECTED_RAW_COLUMNS."""
    audit_cols = {"_batch_id", "_source_file", "_ingested_at", "_execution_mode"}
    business_cols = [c for c in observed_columns if c not in audit_cols]
    observed_set = set(business_cols)
    expected_set = set(RAW_REQUIRED_COLUMNS)

    missing = [c for c in RAW_REQUIRED_COLUMNS if c not in observed_set]
    unexpected = [c for c in business_cols if c not in expected_set]
    return SchemaValidationResult(
        schema_valid=(len(missing) == 0 and len(unexpected) == 0),
        missing_columns=missing,
        unexpected_columns=unexpected,
        observed_columns=business_cols,
    )
