"""Human-in-the-loop approval store for agent recovery recommendations."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from sqlalchemy import desc, select

from datapilot.core.logging import get_logger
from datapilot.database.session import DatabaseManager
from datapilot.models.orm import ApprovalDecisionORM, RecoveryRecommendationORM
from datapilot.schemas.domain import (
    ApprovalDecision,
    RecoveryRecommendation,
    utc_now_iso,
)

logger = get_logger("datapilot.agent.approval")


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_appr_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


class ApprovalStore:
    """Persists pending recovery proposals and human approval audit events."""

    def __init__(
        self,
        approvals_dir: Path,
        db_manager: DatabaseManager | None = None,
    ) -> None:
        self.approvals_dir = approvals_dir
        self.recommendations_dir = approvals_dir / "recommendations"
        self.decisions_dir = approvals_dir / "decisions"
        self.recommendations_dir.mkdir(parents=True, exist_ok=True)
        self.decisions_dir.mkdir(parents=True, exist_ok=True)
        self.db = db_manager

    def save_recommendation(self, recommendation: RecoveryRecommendation) -> Path:
        """Persist a recovery recommendation with pending human approval."""
        if not recommendation.requires_human_approval:
            raise ValueError("Recovery recommendations must always require human approval.")
        if recommendation.auto_executed:
            raise ValueError("Automatic execution of recovery plans is prohibited.")

        path = self.recommendations_dir / f"{recommendation.recommendation_id}.json"
        payload = recommendation.model_dump_json(indent=2)
        _atomic_write(path, payload)

        if self.db is not None:
            with self.db.session_scope() as session:
                existing = session.get(RecoveryRecommendationORM, recommendation.recommendation_id)
                if existing is None:
                    session.add(
                        RecoveryRecommendationORM(
                            recommendation_id=recommendation.recommendation_id,
                            run_id=recommendation.run_id,
                            created_at=recommendation.created_at,
                            failure_category=recommendation.failure_category,
                            approval_status=recommendation.approval_status,
                            approved_by=recommendation.approved_by,
                            payload_json=payload,
                        )
                    )
                else:
                    existing.approval_status = recommendation.approval_status
                    existing.approved_by = recommendation.approved_by
                    existing.payload_json = payload

        logger.info(
            "Saved recovery recommendation=%s run_id=%s status=%s",
            recommendation.recommendation_id,
            recommendation.run_id,
            recommendation.approval_status,
        )
        return path

    def get_recommendation(self, recommendation_id: str) -> RecoveryRecommendation | None:
        """Retrieve a recovery recommendation by ID."""
        if self.db is not None:
            with self.db.session_scope() as session:
                row = session.get(RecoveryRecommendationORM, recommendation_id)
                if row is not None:
                    return RecoveryRecommendation.model_validate_json(row.payload_json)
        path = self.recommendations_dir / f"{recommendation_id}.json"
        if not path.exists():
            return None
        return RecoveryRecommendation.model_validate_json(path.read_text(encoding="utf-8"))

    def list_recommendations(self, run_id: str | None = None) -> list[RecoveryRecommendation]:
        """List stored recovery recommendations newest-first, optionally filtered by run_id."""
        if self.db is not None:
            with self.db.session_scope() as session:
                stmt = select(RecoveryRecommendationORM).order_by(
                    desc(RecoveryRecommendationORM.created_at)
                )
                if run_id is not None:
                    stmt = stmt.where(RecoveryRecommendationORM.run_id == run_id)
                rows = session.scalars(stmt).all()
                if rows:
                    return [RecoveryRecommendation.model_validate_json(r.payload_json) for r in rows]

        items: list[RecoveryRecommendation] = []
        for path in self.recommendations_dir.glob("*.json"):
            try:
                rec = RecoveryRecommendation.model_validate_json(path.read_text(encoding="utf-8"))
                if run_id is None or rec.run_id == run_id:
                    items.append(rec)
            except Exception as exc:
                logger.warning("Skipping unreadable recommendation %s: %s", path.name, exc)
        items.sort(key=lambda r: r.created_at, reverse=True)
        return items

    def record_decision(
        self,
        recommendation_id: str,
        decision: str,
        reviewer: str,
        notes: str = "",
    ) -> RecoveryRecommendation:
        """Record an explicit human APPROVED or REJECTED decision."""
        clean_reviewer = reviewer.strip()
        if not clean_reviewer:
            raise ValueError("Reviewer name is required to record an approval decision.")
        if decision not in ("APPROVED", "REJECTED"):
            raise ValueError(f"Decision must be 'APPROVED' or 'REJECTED', got {decision!r}")

        rec = self.get_recommendation(recommendation_id)
        if rec is None:
            raise FileNotFoundError(f"Recommendation not found: {recommendation_id}")
        if rec.approval_status != "PENDING_APPROVAL":
            raise ValueError(
                f"Recommendation {recommendation_id} already decided as {rec.approval_status}"
            )

        decided_at = utc_now_iso()
        updated = rec.model_copy(
            update={
                "approval_status": decision,
                "approved_by": clean_reviewer,
                "approved_at": decided_at,
                "reviewer_notes": notes.strip() or None,
            }
        )
        updated_payload = updated.model_dump_json(indent=2)
        _atomic_write(
            self.recommendations_dir / f"{recommendation_id}.json",
            updated_payload,
        )

        audit = ApprovalDecision(
            recommendation_id=recommendation_id,
            run_id=updated.run_id,
            decision=decision,  # type: ignore[arg-type]
            reviewer=clean_reviewer,
            notes=notes.strip(),
            decided_at=decided_at,
        )
        audit_payload = audit.model_dump_json(indent=2)
        audit_path = self.decisions_dir / f"{recommendation_id}_{decision.lower()}.json"
        _atomic_write(audit_path, audit_payload)

        if self.db is not None:
            with self.db.session_scope() as session:
                rec_row = session.get(RecoveryRecommendationORM, recommendation_id)
                if rec_row is not None:
                    rec_row.approval_status = updated.approval_status
                    rec_row.approved_by = clean_reviewer
                    rec_row.payload_json = updated_payload
                session.add(
                    ApprovalDecisionORM(
                        recommendation_id=recommendation_id,
                        run_id=updated.run_id,
                        decision=decision,
                        reviewer=clean_reviewer,
                        decided_at=decided_at,
                        payload_json=audit_payload,
                    )
                )

        logger.info(
            "Human decision recorded: rec=%s run=%s decision=%s reviewer=%s",
            recommendation_id,
            updated.run_id,
            decision,
            clean_reviewer,
        )
        return updated

    def approve(
        self, recommendation_id: str, reviewer: str, notes: str = ""
    ) -> RecoveryRecommendation:
        """Approve a pending recovery recommendation."""
        return self.record_decision(recommendation_id, "APPROVED", reviewer, notes=notes)

    def reject(
        self, recommendation_id: str, reviewer: str, notes: str = ""
    ) -> RecoveryRecommendation:
        """Reject a pending recovery recommendation."""
        return self.record_decision(recommendation_id, "REJECTED", reviewer, notes=notes)

    def list_audit_events(self) -> list[ApprovalDecision]:
        """Return all human approval audit events newest-first."""
        if self.db is not None:
            with self.db.session_scope() as session:
                stmt = select(ApprovalDecisionORM).order_by(desc(ApprovalDecisionORM.decided_at))
                rows = session.scalars(stmt).all()
                if rows:
                    return [ApprovalDecision.model_validate_json(r.payload_json) for r in rows]

        events: list[ApprovalDecision] = []
        for path in self.decisions_dir.glob("*.json"):
            try:
                events.append(ApprovalDecision.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception as exc:
                logger.warning("Skipping unreadable approval event %s: %s", path.name, exc)
        events.sort(key=lambda e: e.decided_at, reverse=True)
        return events
