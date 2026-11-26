"""Reproducible synthetic e-commerce sales dataset generator.

Supports clean baseline datasets plus controlled failure-injection scenarios
(null/duplicate spikes, corrupt numeric/date values, schema drift, empty batches)
so the pipeline, data quality engine, and diagnostic AI agent can be tested
deterministically without external dependencies.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

ScenarioName = Literal[
    "clean",
    "dirty_nulls_duplicates",
    "corrupt_prices",
    "schema_drift",
    "empty_file",
]

EXPECTED_RAW_COLUMNS: list[str] = [
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
]

_CATALOG: list[tuple[str, str, str, float]] = [
    ("PROD-1001", "Wireless Noise-Canceling Headphones", "Electronics", 149.99),
    ("PROD-1002", "USB-C Mechanical Keyboard", "Electronics", 89.50),
    ("PROD-1003", "4K Portable Monitor", "Electronics", 279.00),
    ("PROD-2001", "Merino Wool Crewneck Sweater", "Apparel", 64.00),
    ("PROD-2002", "All-Weather Running Jacket", "Apparel", 112.00),
    ("PROD-3001", "Cast Iron Dutch Oven", "Home & Kitchen", 79.95),
    ("PROD-3002", "Precision Pour-Over Kettle", "Home & Kitchen", 54.50),
    ("PROD-4001", "Designing Data-Intensive Systems", "Books", 42.00),
    ("PROD-4002", "Distributed Systems Field Guide", "Books", 36.50),
    ("PROD-5001", "Carbon Trekking Poles", "Sports", 68.00),
    ("PROD-6001", "Vitamin C Barrier Serum", "Beauty", 32.00),
]

_COUNTRIES: list[tuple[str, str, str]] = [
    ("US", "US-East", "USD"),
    ("US", "US-West", "USD"),
    ("CA", "NA-North", "USD"),
    ("GB", "EU-West", "GBP"),
    ("DE", "EU-Central", "EUR"),
    ("IN", "AP-South", "INR"),
    ("SG", "AP-Southeast", "USD"),
]

_PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "UPI", "PAYPAL", "APPLE_PAY"]
_VALID_STATUSES = ["COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "RETURNED"]


def generate_ecommerce_dataset(
    output_path: Path,
    num_rows: int = 250,
    seed: int = 42,
    scenario: ScenarioName = "clean",
) -> Path:
    """Generate a deterministic e-commerce orders CSV at output_path.

    Args:
        output_path: Destination CSV path. Parent directories are created automatically.
        num_rows: Target number of records to generate (ignored when scenario='empty_file').
        seed: Random seed ensuring byte-identical reproducibility across runs.
        scenario: Dataset quality scenario preset.

    Returns:
        Path to the generated CSV file.
    """
    if num_rows < 0:
        raise ValueError(f"num_rows must be non-negative, got {num_rows}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if scenario == "empty_file":
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(EXPECTED_RAW_COLUMNS)
        return output_path

    rng = random.Random(seed)
    base_dt = datetime(2026, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    rows: list[dict[str, str]] = []

    for idx in range(num_rows):
        order_num = 100001 + idx
        prod_id, prod_name, category, base_price = rng.choice(_CATALOG)
        country, region, currency = rng.choice(_COUNTRIES)
        quantity = rng.randint(1, 5)
        price_multiplier = rng.uniform(0.95, 1.08)
        unit_price = round(base_price * price_multiplier, 2)
        discount_pct = rng.choice([0.0, 0.0, 5.0, 10.0, 15.0])
        order_dt = base_dt + timedelta(minutes=rng.randint(0, 60 * 24 * 30))
        status = rng.choices(_VALID_STATUSES, weights=[55, 20, 12, 8, 5], k=1)[0]
        payment = rng.choice(_PAYMENT_METHODS)
        customer_id = f"CUST-{rng.randint(1000, 1180)}"

        # Introduce minor realistic formatting variance in clean mode that Silver normalizes
        cat_text = f" {category.lower()} " if (scenario == "clean" and idx % 17 == 0) else category
        curr_text = currency.lower() if (scenario == "clean" and idx % 19 == 0) else currency

        row = {
            "order_id": f"ORD-{order_num}",
            "customer_id": customer_id,
            "order_timestamp": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "product_id": prod_id,
            "product_name": prod_name,
            "category": cat_text,
            "quantity": str(quantity),
            "unit_price": f"{unit_price:.2f}",
            "discount_pct": f"{discount_pct:.1f}",
            "currency": curr_text,
            "payment_method": payment,
            "order_status": status,
            "country": country,
            "region": region,
        }
        rows.append(row)

    if scenario == "dirty_nulls_duplicates" and rows:
        dup_count = max(1, int(len(rows) * 0.16))
        for i in range(dup_count):
            source_idx = i % max(1, len(rows) // 2)
            dup_row = dict(rows[source_idx])
            dup_row["order_timestamp"] = (base_dt + timedelta(days=32, minutes=i)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            rows[len(rows) - 1 - i] = dup_row

        null_count = max(1, int(len(rows) * 0.14))
        for i in range(null_count):
            target_idx = (i * 3) % len(rows)
            field_choice = i % 3
            if field_choice == 0:
                rows[target_idx]["customer_id"] = ""
            elif field_choice == 1:
                rows[target_idx]["order_timestamp"] = ""
            else:
                rows[target_idx]["unit_price"] = ""

    elif scenario == "corrupt_prices" and rows:
        corrupt_count = max(2, int(len(rows) * 0.22))
        for i in range(corrupt_count):
            target_idx = (i * 2) % len(rows)
            mode = i % 4
            if mode == 0:
                rows[target_idx]["unit_price"] = f"-{15 + i}.99"
            elif mode == 1:
                rows[target_idx]["quantity"] = "0"
            elif mode == 2:
                rows[target_idx]["order_timestamp"] = "NOT_A_TIMESTAMP"
            else:
                rows[target_idx]["order_status"] = "BROKEN_STATUS"

    headers = list(EXPECTED_RAW_COLUMNS)
    if scenario == "schema_drift":
        headers = [
            "order_id",
            # customer_id intentionally dropped
            "order_timestamp",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "item_cost_usd",  # renamed from unit_price
            "discount_pct",
            "currency",
            "payment_method",
            "order_status",
            "country",
            "region",
            "legacy_promo_flag",  # unexpected extra column
        ]
        for row in rows:
            row["item_cost_usd"] = row.pop("unit_price")
            row.pop("customer_id", None)
            row["legacy_promo_flag"] = "N"

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    return output_path
