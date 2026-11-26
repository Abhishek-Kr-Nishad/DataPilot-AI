"""AWS S3, Glue, and Athena inspection & synchronization service."""

from __future__ import annotations

import importlib.util

from datapilot.core.config import AppSettings
from datapilot.metadata.store import MetadataStore
from datapilot.schemas.api import AWSSyncResponse, AthenaSQLResponse
from datapilot.storage.s3_storage import AWSIntegrationError, sync_run_artifacts_to_s3


class AWSService:
    """Handles credential-safe AWS inspection, Athena SQL rendering, and S3 syncing."""

    def __init__(self, settings: AppSettings, metadata_store: MetadataStore) -> None:
        self.settings = settings
        self.metadata_store = metadata_store

    def get_status(self) -> dict[str, object]:
        """Return sanitized AWS configuration and execution mode separation status."""
        return {
            "requested_mode": self.settings.requested_mode,
            "effective_mode": self.settings.effective_mode,
            "fallback_reason": self.settings.fallback_reason,
            "aws": self.settings.aws.sanitized_summary(),
        }

    def get_athena_sql(self) -> AthenaSQLResponse:
        """Load Athena SQL files and render Glue Data Catalog DDL statements."""
        athena_dir = self.settings.local.project_root / "sql" / "athena"
        scripts = (
            {
                path.name: path.read_text(encoding="utf-8")
                for path in sorted(athena_dir.glob("*.sql"))
            }
            if athena_dir.exists()
            else {}
        )
        ddl_path = self.settings.local.project_root / "aws" / "glue_catalog_ddl.py"
        ddl_statements: list[str] = []
        if ddl_path.exists():
            spec = importlib.util.spec_from_file_location("glue_catalog_ddl", ddl_path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                bucket = self.settings.aws.s3_bucket or "<YOUR_BUCKET>"
                prefix = self.settings.aws.s3_prefix.strip("/")
                ddl_statements = mod.render_athena_ddl_statements(
                    self.settings.aws.glue_database,
                    f"s3://{bucket}/{prefix}",
                )
        return AthenaSQLResponse(
            database=self.settings.aws.glue_database,
            scripts=scripts,
            ddl_statements=ddl_statements,
        )

    def sync_run(self, run_id: str) -> AWSSyncResponse:
        """Sync materialized local run artifacts to S3 when AWS is configured."""
        record = self.metadata_store.get_run(run_id)
        if record is None:
            raise FileNotFoundError(f"Pipeline run not found: {run_id}")
        if not self.settings.aws.is_ready_for_aws():
            raise AWSIntegrationError(
                "AWS_S3_BUCKET and valid AWS credentials are required before syncing. "
                "DataPilot AI refuses to simulate AWS sync in local mode."
            )
        updated = sync_run_artifacts_to_s3(self.settings, record)
        self.metadata_store.save_run(updated)
        return AWSSyncResponse(
            run_id=updated.run_id,
            aws_synced=updated.aws_synced,
            s3_bucket=self.settings.aws.s3_bucket or "",
            s3_prefix=self.settings.aws.s3_prefix,
            layers=updated.layers,
        )
