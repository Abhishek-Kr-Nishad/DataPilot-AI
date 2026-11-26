"""CLI utility to sync local Medallion artifacts to AWS S3 and optionally trigger AWS Glue.

Never fakes AWS execution: exits with an explicit error if AWS_S3_BUCKET or
valid AWS credentials are missing.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datapilot.config import load_settings
from datapilot.metadata.store import create_metadata_store
from datapilot.storage.s3_storage import (
    AWSIntegrationError,
    S3StorageBackend,
    sync_run_artifacts_to_s3,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync DataPilot AI run artifacts to AWS S3 / Glue")
    parser.add_argument("--run-id", default=None, help="Run ID to sync (defaults to latest).")
    parser.add_argument(
        "--upload-glue-script",
        action="store_true",
        help="Upload aws/glue_etl_job.py to S3 scripts prefix.",
    )
    parser.add_argument(
        "--trigger-glue-job",
        action="store_true",
        help="Start the configured AWS Glue job after uploading artifacts.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(strict_aws=False)
    if not settings.aws.is_ready_for_aws():
        print(
            "ERROR: AWS credentials or AWS_S3_BUCKET are not configured. "
            "Refusing to report AWS sync without real configuration.",
            file=sys.stderr,
        )
        return 1

    store = create_metadata_store(settings)
    run_record = store.get_run(args.run_id) if args.run_id else (store.list_runs(limit=1) or [None])[0]
    if run_record is None:
        print("ERROR: No pipeline run found to sync.", file=sys.stderr)
        return 1

    try:
        s3_backend = S3StorageBackend.from_aws_config(settings.aws)
        updated = sync_run_artifacts_to_s3(settings, run_record, s3_backend=s3_backend)
        store.save_run(updated)
        print(f"Synced run {updated.run_id} to s3://{settings.aws.s3_bucket}/{settings.aws.s3_prefix}")

        if args.upload_glue_script:
            glue_script = settings.local.project_root / "aws" / "glue_etl_job.py"
            uri = s3_backend.upload_file(glue_script, "scripts/glue_etl_job.py")
            print(f"Uploaded Glue ETL script to {uri}")

        if args.trigger_glue_job:
            glue = boto3.client("glue", region_name=settings.aws.region)
            raw_name = Path(updated.source_file).name
            s3_base = f"s3://{settings.aws.s3_bucket}/{settings.aws.s3_prefix.strip('/')}"
            resp = glue.start_job_run(
                JobName=settings.aws.glue_job_name,
                Arguments={
                    "--S3_INPUT_URI": f"{s3_base}/raw/{updated.batch_id}/{raw_name}",
                    "--S3_BRONZE_URI": f"{s3_base}/bronze/{updated.batch_id}",
                    "--S3_SILVER_URI": f"{s3_base}/silver/{updated.batch_id}",
                    "--S3_QUARANTINE_URI": f"{s3_base}/quarantine/{updated.batch_id}",
                    "--S3_GOLD_URI": f"{s3_base}/gold/{updated.batch_id}",
                    "--BATCH_ID": updated.batch_id,
                },
            )
            print(f"Started AWS Glue job run: {resp['JobRunId']}")
    except AWSIntegrationError as exc:
        print(f"AWS error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
