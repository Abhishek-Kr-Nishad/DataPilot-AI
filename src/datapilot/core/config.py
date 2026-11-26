"""Configuration management for DataPilot AI.

Strictly separates local execution settings from AWS deployment configuration,
loads overrides from environment variables / .env files without hardcoding keys,
and prevents falsely representing local execution as AWS deployment.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field, SecretStr

ExecutionMode = Literal["local", "aws"]
MetadataBackend = Literal["sqlalchemy", "json", "postgres"]
AgentMode = Literal["auto", "mock", "gemini"]


class ConfigurationError(ValueError):
    """Raised when configuration is invalid for the requested execution mode."""


class LocalExecutionConfig(BaseModel):
    """Filesystem and Spark configuration for local development mode."""

    project_root: Path
    data_dir: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    quarantine_dir: Path
    gold_dir: Path
    reports_dir: Path
    approvals_dir: Path
    logs_dir: Path
    spark_master: str = "local[2]"
    spark_app_name: str = "DataPilotAI"
    hadoop_home: str | None = None
    log_level: str = "INFO"


class AWSDeploymentConfig(BaseModel):
    """Configuration specific to AWS S3, Glue, and Athena deployment."""

    region: str = "us-east-1"
    s3_bucket: str | None = None
    s3_prefix: str = "datapilot-ai"
    glue_job_name: str = "datapilot-ecommerce-etl"
    glue_database: str = "datapilot_ecommerce_db"
    athena_output_location: str | None = None
    profile_name: str | None = None
    has_env_credentials: bool = False

    def is_bucket_configured(self) -> bool:
        """Return True if an S3 bucket name has been provided."""
        return bool(self.s3_bucket and self.s3_bucket.strip())

    def has_credential_source(self) -> bool:
        """Return True when environment credentials, AWS profile, or shared config exist."""
        if self.has_env_credentials or self.profile_name:
            return True
        aws_creds_file = Path.home() / ".aws" / "credentials"
        return aws_creds_file.exists()

    def is_ready_for_aws(self) -> bool:
        """Return True when S3 bucket and at least one AWS credential source exist."""
        return self.is_bucket_configured() and self.has_credential_source()

    def sanitized_summary(self) -> dict[str, object]:
        """Return a credential-safe summary suitable for logs and UI display."""
        return {
            "region": self.region,
            "s3_bucket": self.s3_bucket or "(unset)",
            "s3_prefix": self.s3_prefix,
            "glue_job_name": self.glue_job_name,
            "glue_database": self.glue_database,
            "athena_output_location": self.athena_output_location or "(unset)",
            "has_env_credentials": self.has_env_credentials,
            "profile_configured": bool(self.profile_name),
            "ready_for_aws": self.is_ready_for_aws(),
        }


class QualityThresholdConfig(BaseModel):
    """Thresholds controlling data quality pass/fail gates."""

    max_null_rate: float = Field(default=0.05, ge=0.0, le=1.0)
    max_duplicate_rate: float = Field(default=0.05, ge=0.0, le=1.0)
    max_quarantine_rate: float = Field(default=0.15, ge=0.0, le=1.0)
    min_row_count: int = Field(default=1, ge=0)


class MetadataConfig(BaseModel):
    """Database/metadata persistence configuration (SQLite local default or PostgreSQL)."""

    backend: MetadataBackend = "sqlalchemy"
    postgres_dsn: str | None = None
    database_url: str = "sqlite:///data/datapilot_metadata.db"


class AppSettings(BaseModel):
    """Top-level settings object separating requested vs effective mode."""

    requested_mode: ExecutionMode = "local"
    effective_mode: ExecutionMode = "local"
    fallback_reason: str | None = None
    local: LocalExecutionConfig
    aws: AWSDeploymentConfig
    quality: QualityThresholdConfig
    metadata: MetadataConfig
    gemini_model: str = "gemini-2.5-flash"
    has_gemini_key: bool = False
    agent_mode: AgentMode = "auto"
    cors_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://localhost:5173"]
    )
    api_key: SecretStr | None = Field(default=None, exclude=True, repr=False)
    require_auth: bool = False
    max_upload_mb: int = Field(default=25, ge=1, le=500)
    debug: bool = False

    @property
    def auth_enabled(self) -> bool:
        """Return True when API key authentication is enforced."""
        has_key = self.api_key is not None and bool(self.api_key.get_secret_value().strip())
        return self.require_auth or has_key

    @property
    def max_upload_bytes(self) -> int:
        """Maximum allowed CSV upload payload size in bytes."""
        return self.max_upload_mb * 1024 * 1024

    def ensure_directories(self) -> None:
        """Create required local runtime directories if they do not exist."""
        for directory in (
            self.local.data_dir,
            self.local.raw_dir,
            self.local.bronze_dir,
            self.local.silver_dir,
            self.local.quarantine_dir,
            self.local.gold_dir,
            self.local.reports_dir,
            self.local.reports_dir / "runs",
            self.local.reports_dir / "quality",
            self.local.approvals_dir,
            self.local.logs_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


def _clean_env(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _parse_float(name: str, default: float) -> float:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"Environment variable {name} must be a float, got {raw!r}") from exc


def _parse_int(name: str, default: int) -> int:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"Environment variable {name} must be an integer, got {raw!r}") from exc


def _parse_bool(name: str, default: bool) -> bool:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise ConfigurationError(f"Environment variable {name} must be a boolean, got {raw!r}")


def _detect_windows_hadoop_home(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    env_hadoop = _clean_env(os.getenv("HADOOP_HOME"))
    if env_hadoop:
        return env_hadoop
    if os.name == "nt":
        default_winutils = Path(r"C:\Winutils")
        if (default_winutils / "bin" / "winutils.exe").exists():
            return str(default_winutils)
    return None


def _normalize_postgres_url(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+psycopg2://", 1)
    return dsn


def load_settings(
    project_root: Path | None = None,
    env_file: Path | None = None,
    strict_aws: bool = False,
) -> AppSettings:
    """Load application settings from environment variables and optional .env file."""
    resolved_root = (project_root or Path(__file__).resolve().parents[3]).resolve()
    target_env = env_file if env_file is not None else (resolved_root / ".env")
    if target_env.exists():
        load_dotenv(dotenv_path=target_env, override=False)

    requested_raw = (_clean_env(os.getenv("DATAPILOT_EXECUTION_MODE")) or "local").lower()
    if requested_raw not in ("local", "aws"):
        raise ConfigurationError(
            f"DATAPILOT_EXECUTION_MODE must be 'local' or 'aws', got {requested_raw!r}"
        )
    requested_mode: ExecutionMode = requested_raw  # type: ignore[assignment]

    data_dir_cfg = Path(_clean_env(os.getenv("DATAPILOT_DATA_DIR")) or "data")
    data_dir = data_dir_cfg if data_dir_cfg.is_absolute() else (resolved_root / data_dir_cfg)

    local_cfg = LocalExecutionConfig(
        project_root=resolved_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        bronze_dir=data_dir / "bronze",
        silver_dir=data_dir / "silver",
        quarantine_dir=data_dir / "silver" / "quarantine",
        gold_dir=data_dir / "gold",
        reports_dir=data_dir / "reports",
        approvals_dir=data_dir / "approvals",
        logs_dir=resolved_root / "logs",
        spark_master=_clean_env(os.getenv("DATAPILOT_SPARK_MASTER")) or "local[2]",
        spark_app_name=_clean_env(os.getenv("DATAPILOT_SPARK_APP_NAME")) or "DataPilotAI",
        hadoop_home=_detect_windows_hadoop_home(_clean_env(os.getenv("DATAPILOT_HADOOP_HOME"))),
        log_level=(_clean_env(os.getenv("DATAPILOT_LOG_LEVEL")) or "INFO").upper(),
    )

    has_env_creds = bool(
        _clean_env(os.getenv("AWS_ACCESS_KEY_ID")) and _clean_env(os.getenv("AWS_SECRET_ACCESS_KEY"))
    )
    aws_cfg = AWSDeploymentConfig(
        region=_clean_env(os.getenv("AWS_REGION")) or "us-east-1",
        s3_bucket=_clean_env(os.getenv("AWS_S3_BUCKET")),
        s3_prefix=_clean_env(os.getenv("AWS_S3_PREFIX")) or "datapilot-ai",
        glue_job_name=_clean_env(os.getenv("AWS_GLUE_JOB_NAME")) or "datapilot-ecommerce-etl",
        glue_database=_clean_env(os.getenv("AWS_GLUE_DATABASE")) or "datapilot_ecommerce_db",
        athena_output_location=_clean_env(os.getenv("AWS_ATHENA_OUTPUT_LOCATION")),
        profile_name=_clean_env(os.getenv("AWS_PROFILE")),
        has_env_credentials=has_env_creds,
    )

    fallback_reason: str | None = None
    effective_mode: ExecutionMode = requested_mode
    if requested_mode == "aws" and not aws_cfg.is_ready_for_aws():
        reason = (
            "AWS execution mode was requested, but AWS_S3_BUCKET and/or AWS credentials "
            "are unavailable. Falling back to local development mode."
        )
        if strict_aws:
            raise ConfigurationError(reason)
        effective_mode = "local"
        fallback_reason = reason

    quality_cfg = QualityThresholdConfig(
        max_null_rate=_parse_float("DATAPILOT_MAX_NULL_RATE", 0.05),
        max_duplicate_rate=_parse_float("DATAPILOT_MAX_DUPLICATE_RATE", 0.05),
        max_quarantine_rate=_parse_float("DATAPILOT_MAX_QUARANTINE_RATE", 0.15),
        min_row_count=_parse_int("DATAPILOT_MIN_ROW_COUNT", 1),
    )

    metadata_backend_raw = (
        _clean_env(os.getenv("DATAPILOT_METADATA_BACKEND")) or "sqlalchemy"
    ).lower()
    if metadata_backend_raw not in ("sqlalchemy", "json", "postgres"):
        raise ConfigurationError(
            "DATAPILOT_METADATA_BACKEND must be 'sqlalchemy', 'json', or 'postgres', "
            f"got {metadata_backend_raw!r}"
        )
    metadata_backend: MetadataBackend = metadata_backend_raw  # type: ignore[assignment]
    postgres_dsn = _clean_env(os.getenv("POSTGRES_DSN"))
    explicit_db_url = _clean_env(os.getenv("DATABASE_URL"))

    if metadata_backend == "postgres" and not postgres_dsn and not explicit_db_url:
        raise ConfigurationError(
            "POSTGRES_DSN or DATABASE_URL must be set when DATAPILOT_METADATA_BACKEND='postgres'"
        )

    if explicit_db_url:
        database_url = _normalize_postgres_url(explicit_db_url)
    elif postgres_dsn:
        database_url = _normalize_postgres_url(postgres_dsn)
    else:
        sqlite_file = (data_dir / "datapilot_metadata.db").as_posix()
        database_url = f"sqlite:///{sqlite_file}"

    agent_mode_raw = (_clean_env(os.getenv("DATAPILOT_AGENT_MODE")) or "auto").lower()
    if agent_mode_raw not in ("auto", "mock", "gemini"):
        raise ConfigurationError(
            f"DATAPILOT_AGENT_MODE must be 'auto', 'mock', or 'gemini', got {agent_mode_raw!r}"
        )
    agent_mode: AgentMode = agent_mode_raw  # type: ignore[assignment]

    cors_raw = _clean_env(os.getenv("DATAPILOT_CORS_ORIGINS")) or "http://localhost:3000,http://localhost:5173"
    cors_origins = [origin.strip() for origin in cors_raw.split(",") if origin.strip()]

    raw_api_key = _clean_env(os.getenv("DATAPILOT_API_KEY"))
    require_auth = _parse_bool("DATAPILOT_REQUIRE_AUTH", False)
    max_upload_mb = _parse_int("DATAPILOT_MAX_UPLOAD_MB", 25)
    if max_upload_mb < 1 or max_upload_mb > 500:
        raise ConfigurationError(
            f"DATAPILOT_MAX_UPLOAD_MB must be between 1 and 500, got {max_upload_mb}"
        )
    debug_mode = _parse_bool("DATAPILOT_DEBUG", False)

    return AppSettings(
        requested_mode=requested_mode,
        effective_mode=effective_mode,
        fallback_reason=fallback_reason,
        local=local_cfg,
        aws=aws_cfg,
        quality=quality_cfg,
        metadata=MetadataConfig(
            backend=metadata_backend,
            postgres_dsn=postgres_dsn,
            database_url=database_url,
        ),
        gemini_model=_clean_env(os.getenv("DATAPILOT_GEMINI_MODEL")) or "gemini-2.5-flash",
        has_gemini_key=bool(_clean_env(os.getenv("GEMINI_API_KEY"))),
        agent_mode=agent_mode,
        cors_origins=cors_origins,
        api_key=SecretStr(raw_api_key) if raw_api_key else None,
        require_auth=require_auth,
        max_upload_mb=max_upload_mb,
        debug=debug_mode,
    )
