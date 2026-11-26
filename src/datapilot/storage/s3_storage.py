"""AWS S3 storage backend using boto3 with strict credential safety."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from datapilot.config import AWSDeploymentConfig, AppSettings
from datapilot.logging_utils import get_logger
from datapilot.metadata.models import PipelineRunRecord
from datapilot.storage.base import StorageBackend

logger = get_logger("datapilot.storage.s3")


class AWSIntegrationError(RuntimeError):
    """Raised when an AWS S3/Glue operation cannot run due to config or API failure."""


class S3StorageBackend(StorageBackend):
    """Boto3-backed S3 storage implementation for Medallion artifacts."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "datapilot-ai",
        region: str = "us-east-1",
        profile_name: str | None = None,
        s3_client: Any | None = None,
    ) -> None:
        if not bucket or not bucket.strip():
            raise AWSIntegrationError("AWS S3 bucket name must be configured.")
        self.bucket = bucket.strip()
        self.prefix = prefix.strip("/")
        self.region = region
        self.profile_name = profile_name

        if s3_client is not None:
            self.client = s3_client
        else:
            session = (
                boto3.Session(profile_name=profile_name, region_name=region)
                if profile_name
                else boto3.Session(region_name=region)
            )
            self.client = session.client("s3")

    @classmethod
    def from_aws_config(cls, config: AWSDeploymentConfig) -> "S3StorageBackend":
        """Construct S3StorageBackend from AWSDeploymentConfig."""
        if not config.is_bucket_configured():
            raise AWSIntegrationError(
                "AWS_S3_BUCKET is not set. Configure AWS_S3_BUCKET in environment or .env."
            )
        return cls(
            bucket=config.s3_bucket or "",
            prefix=config.s3_prefix,
            region=config.region,
            profile_name=config.profile_name,
        )

    def _full_key(self, remote_key: str) -> str:
        clean = remote_key.strip("/").replace("\\", "/")
        return f"{self.prefix}/{clean}" if self.prefix and clean else (self.prefix or clean)

    def check_bucket_access(self) -> bool:
        """Verify credentials and bucket existence via HeadBucket."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except NoCredentialsError as exc:
            raise AWSIntegrationError(
                "AWS credentials were not found in environment, profile, or IAM role."
            ) from exc
        except (ClientError, BotoCoreError) as exc:
            raise AWSIntegrationError(
                f"Failed to access S3 bucket s3://{self.bucket}: {exc}"
            ) from exc

    def upload_file(self, local_path: Path, remote_key: str) -> str:
        if not local_path.exists():
            raise FileNotFoundError(f"Local file does not exist: {local_path}")
        key = self._full_key(remote_key)
        try:
            with local_path.open("rb") as handle:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=handle.read())
        except NoCredentialsError as exc:
            raise AWSIntegrationError("AWS credentials unavailable for S3 upload.") from exc
        except (ClientError, BotoCoreError) as exc:
            raise AWSIntegrationError(f"Failed uploading {local_path} to s3://{self.bucket}/{key}: {exc}") from exc
        uri = f"s3://{self.bucket}/{key}"
        logger.info("Uploaded %s -> %s", local_path.name, uri)
        return uri

    def upload_directory(self, local_dir: Path, remote_prefix: str) -> list[str]:
        if not local_dir.exists():
            raise FileNotFoundError(f"Local directory does not exist: {local_dir}")
        uris: list[str] = []
        clean_prefix = remote_prefix.strip("/").replace("\\", "/")
        for file_path in sorted(local_dir.rglob("*")):
            if file_path.is_file():
                rel = file_path.relative_to(local_dir).as_posix()
                sub_key = f"{clean_prefix}/{rel}" if clean_prefix else rel
                uris.append(self.upload_file(file_path, sub_key))
        return uris

    def download_file(self, remote_key: str, local_path: Path) -> Path:
        key = self._full_key(remote_key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            data = response["Body"].read()
            local_path.write_bytes(data)
        except NoCredentialsError as exc:
            raise AWSIntegrationError("AWS credentials unavailable for S3 download.") from exc
        except (ClientError, BotoCoreError) as exc:
            raise AWSIntegrationError(
                f"Failed downloading s3://{self.bucket}/{key}: {exc}"
            ) from exc
        return local_path

    def list_objects(self, prefix: str) -> list[str]:
        full_prefix = self._full_key(prefix)
        keys: list[str] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
        except NoCredentialsError as exc:
            raise AWSIntegrationError("AWS credentials unavailable for S3 listing.") from exc
        except (ClientError, BotoCoreError) as exc:
            raise AWSIntegrationError(
                f"Failed listing s3://{self.bucket}/{full_prefix}: {exc}"
            ) from exc
        return sorted(keys)

    def exists(self, remote_key: str) -> bool:
        key = self._full_key(remote_key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise AWSIntegrationError(f"Error checking s3://{self.bucket}/{key}: {exc}") from exc
        except NoCredentialsError as exc:
            raise AWSIntegrationError("AWS credentials unavailable for head_object.") from exc

    def uri_for(self, remote_key: str) -> str:
        return f"s3://{self.bucket}/{self._full_key(remote_key)}"


def sync_run_artifacts_to_s3(
    settings: AppSettings,
    run_record: PipelineRunRecord,
    s3_backend: S3StorageBackend | None = None,
) -> PipelineRunRecord:
    """Upload materialized local run layers and reports to AWS S3.

    Updates LayerMetrics.s3_uri and marks aws_synced=True only after uploads succeed.
    """
    backend = s3_backend or S3StorageBackend.from_aws_config(settings.aws)
    backend.check_bucket_access()

    source_path = Path(run_record.source_file)
    if source_path.exists():
        backend.upload_file(source_path, f"raw/{run_record.batch_id}/{source_path.name}")

    updated_layers = {}
    for layer_key, metrics in run_record.layers.items():
        local_layer_path = Path(metrics.path)
        s3_prefix = f"{metrics.layer.replace('.', '/')}/{run_record.batch_id}"
        if local_layer_path.exists():
            backend.upload_directory(local_layer_path, s3_prefix)
        updated_layers[layer_key] = metrics.model_copy(
            update={"s3_uri": backend.uri_for(s3_prefix)}
        )

    if run_record.quality_report_path:
        q_path = Path(run_record.quality_report_path)
        if q_path.exists():
            backend.upload_file(q_path, f"reports/quality/{q_path.name}")

    return run_record.model_copy(
        update={
            "layers": updated_layers,
            "aws_synced": True,
        }
    )
