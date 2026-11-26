"""Metadata persistence backends (SQLAlchemy ORM for PostgreSQL/SQLite + JSON)."""

from __future__ import annotations

import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from sqlalchemy import desc, select

from datapilot.core.config import AppSettings
from datapilot.core.logging import get_logger
from datapilot.database.session import DatabaseManager, create_database_manager
from datapilot.models.orm import PipelineRunORM, QualityReportORM
from datapilot.schemas.domain import PipelineRunRecord, QualityReport

logger = get_logger("datapilot.metadata.store")


class MetadataStore(ABC):
    """Abstract store for pipeline run manifests and quality reports."""

    @abstractmethod
    def save_run(self, record: PipelineRunRecord) -> str:
        """Persist or update a pipeline run manifest and return its locator."""

    @abstractmethod
    def get_run(self, run_id: str) -> PipelineRunRecord | None:
        """Fetch a single pipeline run manifest by run_id."""

    @abstractmethod
    def list_runs(self, limit: int = 50) -> list[PipelineRunRecord]:
        """Return pipeline runs sorted newest-first."""

    @abstractmethod
    def save_quality_report(self, report: QualityReport) -> str:
        """Persist a quality report and return its locator."""

    @abstractmethod
    def get_quality_report(self, run_id: str) -> QualityReport | None:
        """Fetch a quality report by run_id."""


def _atomic_write_text(target: Path, content: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(target.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, target)
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


class LocalMetadataStore(MetadataStore):
    """JSON-backed metadata repository stored under data/reports."""

    def __init__(self, reports_dir: Path) -> None:
        self.reports_dir = reports_dir
        self.runs_dir = reports_dir / "runs"
        self.quality_dir = reports_dir / "quality"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.quality_dir.mkdir(parents=True, exist_ok=True)

    def save_run(self, record: PipelineRunRecord) -> str:
        path = self.runs_dir / f"{record.run_id}.json"
        _atomic_write_text(path, record.model_dump_json(indent=2))
        return str(path)

    def get_run(self, run_id: str) -> PipelineRunRecord | None:
        path = self.runs_dir / f"{run_id}.json"
        if not path.exists():
            return None
        return PipelineRunRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_runs(self, limit: int = 50) -> list[PipelineRunRecord]:
        records: list[PipelineRunRecord] = []
        for path in self.runs_dir.glob("*.json"):
            try:
                records.append(PipelineRunRecord.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception as exc:
                logger.warning("Skipping unreadable run manifest %s: %s", path.name, exc)
        records.sort(key=lambda r: r.started_at, reverse=True)
        return records[:limit]

    def save_quality_report(self, report: QualityReport) -> str:
        path = self.quality_dir / f"{report.run_id}.json"
        _atomic_write_text(path, report.model_dump_json(indent=2))
        return str(path)

    def get_quality_report(self, run_id: str) -> QualityReport | None:
        path = self.quality_dir / f"{run_id}.json"
        if not path.exists():
            return None
        return QualityReport.model_validate_json(path.read_text(encoding="utf-8"))


class SQLAlchemyMetadataStore(MetadataStore):
    """SQLAlchemy ORM metadata repository (PostgreSQL or local SQLite) with JSON export."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        reports_dir: Path | None = None,
    ) -> None:
        self.db = db_manager
        self.json_mirror = LocalMetadataStore(reports_dir) if reports_dir is not None else None

    def save_run(self, record: PipelineRunRecord) -> str:
        payload = record.model_dump_json()
        with self.db.session_scope() as session:
            existing = session.get(PipelineRunORM, record.run_id)
            if existing is None:
                session.add(
                    PipelineRunORM(
                        run_id=record.run_id,
                        batch_id=record.batch_id,
                        started_at=record.started_at,
                        completed_at=record.completed_at,
                        status=record.status,
                        requested_mode=record.requested_mode,
                        effective_mode=record.effective_mode,
                        aws_synced=record.aws_synced,
                        scenario=record.scenario,
                        duration_seconds=record.duration_seconds,
                        payload_json=payload,
                    )
                )
            else:
                existing.completed_at = record.completed_at
                existing.status = record.status
                existing.effective_mode = record.effective_mode
                existing.aws_synced = record.aws_synced
                existing.duration_seconds = record.duration_seconds
                existing.payload_json = payload

        if self.json_mirror is not None:
            return self.json_mirror.save_run(record)
        return f"db://datapilot_pipeline_runs/{record.run_id}"

    def get_run(self, run_id: str) -> PipelineRunRecord | None:
        with self.db.session_scope() as session:
            row = session.get(PipelineRunORM, run_id)
            if row is not None:
                try:
                    return PipelineRunRecord.model_validate_json(row.payload_json)
                except Exception as exc:
                    logger.warning("Corrupted DB run manifest %s: %s", run_id, exc)
                    return None
        if self.json_mirror is not None:
            try:
                return self.json_mirror.get_run(run_id)
            except Exception as exc:
                logger.warning("Corrupted JSON run manifest %s: %s", run_id, exc)
        return None

    def list_runs(self, limit: int = 50) -> list[PipelineRunRecord]:
        with self.db.session_scope() as session:
            stmt = (
                select(PipelineRunORM)
                .order_by(desc(PipelineRunORM.started_at))
                .limit(limit)
            )
            rows = session.scalars(stmt).all()
            if rows:
                results: list[PipelineRunRecord] = []
                for row in rows:
                    try:
                        results.append(PipelineRunRecord.model_validate_json(row.payload_json))
                    except Exception as exc:
                        logger.warning(
                            "Skipping invalid DB run manifest %s: %s", row.run_id, exc
                        )
                return results
        if self.json_mirror is not None:
            return self.json_mirror.list_runs(limit=limit)
        return []

    def save_quality_report(self, report: QualityReport) -> str:
        payload = report.model_dump_json()
        with self.db.session_scope() as session:
            existing = session.get(QualityReportORM, report.run_id)
            if existing is None:
                session.add(
                    QualityReportORM(
                        run_id=report.run_id,
                        dataset_name=report.dataset_name,
                        generated_at=report.generated_at,
                        overall_passed=report.overall_passed,
                        quality_score=report.quality_score,
                        total_bronze_rows=report.total_bronze_rows,
                        valid_silver_rows=report.valid_silver_rows,
                        quarantined_rows=report.quarantined_rows,
                        duplicate_rows_removed=report.duplicate_rows_removed,
                        payload_json=payload,
                    )
                )
            else:
                existing.overall_passed = report.overall_passed
                existing.quality_score = report.quality_score
                existing.valid_silver_rows = report.valid_silver_rows
                existing.quarantined_rows = report.quarantined_rows
                existing.duplicate_rows_removed = report.duplicate_rows_removed
                existing.payload_json = payload

        if self.json_mirror is not None:
            return self.json_mirror.save_quality_report(report)
        return f"db://datapilot_quality_reports/{report.run_id}"

    def get_quality_report(self, run_id: str) -> QualityReport | None:
        with self.db.session_scope() as session:
            row = session.get(QualityReportORM, run_id)
            if row is not None:
                return QualityReport.model_validate_json(row.payload_json)
        if self.json_mirror is not None:
            return self.json_mirror.get_quality_report(run_id)
        return None


PostgresMetadataStore = SQLAlchemyMetadataStore


def create_metadata_store(
    settings: AppSettings,
    db_manager: DatabaseManager | None = None,
) -> MetadataStore:
    """Instantiate the configured metadata store backend."""
    if settings.metadata.backend == "json":
        return LocalMetadataStore(settings.local.reports_dir)
    manager = db_manager or create_database_manager(settings)
    return SQLAlchemyMetadataStore(manager, reports_dir=settings.local.reports_dir)
