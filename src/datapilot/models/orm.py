"""SQLAlchemy 2.0 ORM definitions for pipeline runs, quality reports, and approvals."""

from __future__ import annotations

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all DataPilot AI relational tables."""


class PipelineRunORM(Base):
    """Relational table storing pipeline execution metadata."""

    __tablename__ = "datapilot_pipeline_runs"

    run_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    started_at: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    completed_at: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    requested_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    effective_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    aws_synced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    scenario: Mapped[str] = mapped_column(String(64), nullable=False, default="clean")
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())


class QualityReportORM(Base):
    """Relational table storing data quality report metrics and assertions."""

    __tablename__ = "datapilot_quality_reports"

    run_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    dataset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    generated_at: Mapped[str] = mapped_column(String(64), nullable=False)
    overall_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_bronze_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    valid_silver_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quarantined_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_rows_removed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)


class RecoveryRecommendationORM(Base):
    """Relational table storing AI-proposed recovery plans awaiting human approval."""

    __tablename__ = "datapilot_recovery_recommendations"

    recommendation_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    created_at: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    failure_category: Mapped[str] = mapped_column(String(80), nullable=False)
    approval_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    approved_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)


class ApprovalDecisionORM(Base):
    """Immutable audit log of human reviewer approval and rejection actions."""

    __tablename__ = "datapilot_approval_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recommendation_id: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    reviewer: Mapped[str] = mapped_column(String(120), nullable=False)
    decided_at: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
