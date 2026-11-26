"""CLI entrypoint to generate synthetic e-commerce data and run the local pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datapilot.config import load_settings
from datapilot.data_generator import generate_ecommerce_dataset
from datapilot.pipeline.runner import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DataPilot AI local PySpark pipeline")
    parser.add_argument(
        "--scenario",
        choices=["clean", "dirty_nulls_duplicates", "corrupt_prices", "schema_drift", "empty_file"],
        default="clean",
        help="Synthetic dataset preset to generate when --input-csv is omitted.",
    )
    parser.add_argument("--rows", type=int, default=250, help="Number of synthetic orders.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--input-csv", type=Path, default=None, help="Existing CSV path to ingest.")
    parser.add_argument(
        "--allow-quality-warnings",
        action="store_true",
        help="Continue to Gold layer even if quality checks fail.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings()
    settings.ensure_directories()

    if args.input_csv is not None:
        csv_path = args.input_csv
    else:
        csv_path = settings.local.raw_dir / f"orders_{args.scenario}_seed{args.seed}.csv"
        generate_ecommerce_dataset(
            output_path=csv_path,
            num_rows=args.rows,
            seed=args.seed,
            scenario=args.scenario,
        )

    result = run_pipeline(
        settings=settings,
        raw_csv_path=csv_path,
        scenario=args.scenario,
        fail_on_quality_error=not args.allow_quality_warnings,
    )
    rec = result.run_record
    print(f"Run ID         : {rec.run_id}")
    print(f"Status         : {rec.status}")
    print(f"Effective Mode : {rec.effective_mode} (AWS synced: {rec.aws_synced})")
    print(f"Duration       : {rec.duration_seconds}s")
    for layer_name, metrics in rec.layers.items():
        print(f"  - {layer_name:<25} rows={metrics.row_count:<6} path={metrics.path}")
    if result.quality_report is not None:
        print(f"Quality        : {result.quality_report.summary}")
    if rec.error_message:
        print(f"Error          : {rec.error_type}: {rec.error_message}")
    return 0 if rec.status == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
