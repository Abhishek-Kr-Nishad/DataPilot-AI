"""Validated, non-destructive diagnostic tools for the DataPilot AI agent.

Implements the five required tools:
1. inspect_pipeline_status()
2. inspect_quality_report()
3. identify_failure_reason()
4. generate_sql_validation()
5. recommend_recovery()
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from datapilot.agent.approval import ApprovalStore
from datapilot.config import AppSettings
from datapilot.logging_utils import get_logger
from datapilot.metadata.models import (
    FailureDiagnosis,
    PipelineRunRecord,
    QualityReport,
    RecoveryAction,
    RecoveryRecommendation,
    SQLValidationPack,
    utc_now_iso,
)
from datapilot.metadata.store import MetadataStore, create_metadata_store

_SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|INSERT|UPDATE|ALTER|CREATE|MERGE|REPLACE|GRANT|REVOKE|MSCK)\b",
    re.IGNORECASE,
)


def _validate_identifier(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} cannot be blank.")
    if not _SAFE_ID_PATTERN.match(cleaned):
        raise ValueError(f"{field_name} contains unsafe characters: {value!r}")
    return cleaned


def validate_read_only_sql(sql: str) -> bool:
    """Verify that generated validation SQL is strictly read-only."""
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL validation query cannot be empty.")
    first_token = stripped.split(None, 1)[0].upper()
    if first_token not in {"SELECT", "WITH", "SHOW", "DESCRIBE"}:
        raise ValueError(f"Only read-only SQL statements are allowed, found {first_token!r}")
    match = _FORBIDDEN_SQL_KEYWORDS.search(stripped)
    if match:
        raise ValueError(f"Mutating SQL keyword {match.group(1)!r} is prohibited in validation SQL")
    return True


class DiagnosticToolbox:
    """Suite of validated, logged, read-only/advisory tools for pipeline diagnostics."""

    def __init__(
        self,
        settings: AppSettings,
        metadata_store: MetadataStore | None = None,
        approval_store: ApprovalStore | None = None,
    ) -> None:
        self.settings = settings
        self.metadata_store = metadata_store or create_metadata_store(settings)
        self.approval_store = approval_store or ApprovalStore(settings.local.approvals_dir)
        self.logger = get_logger(
            "datapilot.agent.tools",
            level=settings.local.log_level,
            log_file=settings.local.logs_dir / "agent_tools.log",
        )
        self.tool_trace: list[dict[str, str]] = []

    def _record_trace(self, tool_name: str, run_id: str | None, outcome: str) -> None:
        entry = {
            "timestamp": utc_now_iso(),
            "tool": tool_name,
            "run_id": run_id or "latest",
            "outcome": outcome,
        }
        self.tool_trace.append(entry)
        self.logger.info("TOOL_CALL tool=%s run_id=%s outcome=%s", tool_name, run_id or "latest", outcome)

    def _resolve_run(self, run_id: str | None) -> PipelineRunRecord | None:
        safe_id = _validate_identifier(run_id, "run_id")
        if safe_id is not None:
            return self.metadata_store.get_run(safe_id)
        runs = self.metadata_store.list_runs(limit=1)
        return runs[0] if runs else None

    def _read_log_tail(self, run_id: str, max_lines: int = 20) -> list[str]:
        log_path = self.settings.local.logs_dir / f"{run_id}.log"
        if not log_path.exists():
            return []
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-max_lines:]

    def _sample_quarantine_reasons(self, run: PipelineRunRecord, limit: int = 5) -> dict[str, int]:
        quarantine_layer = run.layers.get("quarantine")
        if quarantine_layer is None:
            return {}
        q_path = Path(quarantine_layer.path)
        if not q_path.exists():
            return {}
        try:
            table = pq.read_table(q_path, columns=["_quarantine_reason"])
            reasons = table.column("_quarantine_reason").to_pylist()
            counts: dict[str, int] = {}
            for item in reasons:
                key = str(item or "unknown")
                counts[key] = counts.get(key, 0) + 1
            top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]
            return dict(top)
        except Exception as exc:
            self.logger.warning("Unable to inspect quarantine Parquet at %s: %s", q_path, exc)
            return {}

    def inspect_pipeline_status(self, run_id: str | None = None) -> dict[str, Any]:
        """Inspect pipeline run manifest, execution mode, layer metrics, and recent history."""
        run = self._resolve_run(run_id)
        recent = self.metadata_store.list_runs(limit=5)
        if run is None:
            self._record_trace("inspect_pipeline_status", run_id, "NOT_FOUND")
            return {
                "found": False,
                "run_id": run_id,
                "message": "No pipeline runs found in metadata store.",
                "recent_runs": [],
            }

        payload = {
            "found": True,
            "run_id": run.run_id,
            "batch_id": run.batch_id,
            "status": run.status,
            "requested_mode": run.requested_mode,
            "effective_mode": run.effective_mode,
            "aws_synced": run.aws_synced,
            "fallback_reason": run.fallback_reason,
            "source_file": run.source_file,
            "scenario": run.scenario,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "duration_seconds": run.duration_seconds,
            "error_type": run.error_type,
            "error_message": run.error_message,
            "layers": {name: m.model_dump() for name, m in run.layers.items()},
            "notes": run.notes,
            "log_tail": self._read_log_tail(run.run_id),
            "recent_run_ids": [r.run_id for r in recent],
        }
        self._record_trace("inspect_pipeline_status", run.run_id, f"OK:{run.status}")
        return payload

    def inspect_quality_report(self, run_id: str | None = None) -> dict[str, Any]:
        """Inspect the data quality report and top quarantine failure reasons for a run."""
        run = self._resolve_run(run_id)
        if run is None:
            self._record_trace("inspect_quality_report", run_id, "RUN_NOT_FOUND")
            return {
                "found": False,
                "run_id": run_id,
                "message": "No pipeline run found to inspect quality report.",
            }

        report: QualityReport | None = self.metadata_store.get_quality_report(run.run_id)
        if report is None:
            self._record_trace("inspect_quality_report", run.run_id, "REPORT_NOT_FOUND")
            return {
                "found": False,
                "run_id": run.run_id,
                "message": (
                    f"No quality report found for {run.run_id} "
                    f"(pipeline status={run.status}, error={run.error_type})."
                ),
            }

        failed_checks = [c.model_dump() for c in report.checks if not c.passed]
        if report.quarantined_rows == 0:
            quarantine_breakdown = {}
        elif report.quarantine_reason_counts:
            quarantine_breakdown = dict(report.quarantine_reason_counts)
        else:
            quarantine_breakdown = self._sample_quarantine_reasons(run)
        result = {
            "found": True,
            "run_id": report.run_id,
            "dataset_name": report.dataset_name,
            "overall_passed": report.overall_passed,
            "summary": report.summary,
            "total_bronze_rows": report.total_bronze_rows,
            "valid_silver_rows": report.valid_silver_rows,
            "quarantined_rows": report.quarantined_rows,
            "duplicate_rows_removed": report.duplicate_rows_removed,
            "schema_valid": report.schema_valid,
            "missing_columns": report.missing_columns,
            "unexpected_columns": report.unexpected_columns,
            "null_counts": report.null_counts,
            "null_rates": report.null_rates,
            "failed_checks": failed_checks,
            "all_checks": [c.model_dump() for c in report.checks],
            "top_quarantine_reasons": quarantine_breakdown,
        }
        self._record_trace(
            "inspect_quality_report",
            run.run_id,
            f"OK:passed={report.overall_passed}",
        )
        return result

    def identify_failure_reason(self, run_id: str | None = None) -> FailureDiagnosis:
        """Analyze pipeline status and quality metrics to pinpoint root cause."""
        status_info = self.inspect_pipeline_status(run_id=run_id)
        if not status_info["found"]:
            target = run_id or "none"
            diag = FailureDiagnosis(
                run_id=target,
                status_observed="UNKNOWN",
                failure_category="MISSING_RUN_METADATA",
                severity="HIGH",
                root_cause="No pipeline run manifest exists for the requested run_id.",
                evidence=[f"Metadata store returned no record for run_id={target!r}."],
                affected_layers=[],
                confidence=0.95,
            )
            self._record_trace("identify_failure_reason", target, diag.failure_category)
            return diag

        resolved_run_id = str(status_info["run_id"])
        status = str(status_info["status"])
        error_type = status_info.get("error_type")
        error_msg = status_info.get("error_message") or ""
        quality_info = self.inspect_quality_report(run_id=resolved_run_id)

        if status == "SUCCESS" and quality_info.get("overall_passed", True):
            diag = FailureDiagnosis(
                run_id=resolved_run_id,
                status_observed=status,
                failure_category="HEALTHY_RUN",
                severity="LOW",
                root_cause="Pipeline completed cleanly across Bronze, Silver, Quality, and Gold layers.",
                evidence=[
                    f"Bronze rows={status_info['layers'].get('bronze', {}).get('row_count', 0)}",
                    f"Silver rows={status_info['layers'].get('silver', {}).get('row_count', 0)}",
                    f"Quality summary: {quality_info.get('summary', 'PASSED')}",
                ],
                affected_layers=[],
                confidence=0.99,
            )
            self._record_trace("identify_failure_reason", resolved_run_id, diag.failure_category)
            return diag

        # Hard failure before/during Silver
        if error_type == "SchemaDriftError" or (
            quality_info.get("found") and quality_info.get("missing_columns")
        ):
            missing = quality_info.get("missing_columns", [])
            unexpected = quality_info.get("unexpected_columns", [])
            diag = FailureDiagnosis(
                run_id=resolved_run_id,
                status_observed=status,
                failure_category="SCHEMA_DRIFT",
                severity="CRITICAL",
                root_cause=(
                    "Upstream CSV schema drifted from the Silver contract: required columns "
                    f"{missing} are missing (unexpected columns: {unexpected})."
                ),
                evidence=[
                    f"error_type={error_type}",
                    f"missing_columns={missing}",
                    f"unexpected_columns={unexpected}",
                    f"source_file={status_info.get('source_file')}",
                ],
                affected_layers=["bronze", "silver", "gold"],
                confidence=0.98,
            )
            self._record_trace("identify_failure_reason", resolved_run_id, diag.failure_category)
            return diag

        if error_type == "FileNotFoundError":
            diag = FailureDiagnosis(
                run_id=resolved_run_id,
                status_observed=status,
                failure_category="MISSING_SOURCE_FILE",
                severity="HIGH",
                root_cause=f"Raw input CSV could not be located: {error_msg}",
                evidence=[f"source_file={status_info.get('source_file')}", error_msg],
                affected_layers=["bronze", "silver", "gold"],
                confidence=0.99,
            )
            self._record_trace("identify_failure_reason", resolved_run_id, diag.failure_category)
            return diag

        if quality_info.get("found"):
            failed_checks = quality_info.get("failed_checks", [])
            failed_names = {c["check_name"] for c in failed_checks}
            bronze_rows = int(quality_info.get("total_bronze_rows", 0))
            silver_rows = int(quality_info.get("valid_silver_rows", 0))
            quarantined = int(quality_info.get("quarantined_rows", 0))
            duplicates = int(quality_info.get("duplicate_rows_removed", 0))
            q_reasons = quality_info.get("top_quarantine_reasons", {})

            if "minimum_row_count" in failed_names or bronze_rows == 0:
                diag = FailureDiagnosis(
                    run_id=resolved_run_id,
                    status_observed=status,
                    failure_category="EMPTY_INPUT_DATASET",
                    severity="HIGH",
                    root_cause=(
                        "Ingested raw CSV contained 0 data records, tripping minimum_row_count "
                        "and blocking downstream Silver/Gold updates."
                    ),
                    evidence=[
                        f"total_bronze_rows={bronze_rows}",
                        f"source_file={status_info.get('source_file')}",
                    ],
                    affected_layers=["bronze", "silver", "gold"],
                    confidence=0.97,
                )
            elif (
                "duplicate_order_rate" in failed_names
                and "quarantine_ratio" not in failed_names
                and "critical_null_rate" not in failed_names
            ):
                dup_rate = duplicates / max(bronze_rows, 1)
                diag = FailureDiagnosis(
                    run_id=resolved_run_id,
                    status_observed=status,
                    failure_category="EXCESSIVE_DUPLICATES",
                    severity="HIGH",
                    root_cause=(
                        f"Duplicate order_id rate ({dup_rate:.1%}) exceeded threshold, indicating "
                        "replayed/double-delivered upstream batches."
                    ),
                    evidence=[
                        f"duplicate_rows_removed={duplicates} of {bronze_rows}",
                        f"failed_checks={[c['check_name'] for c in failed_checks]}",
                    ],
                    affected_layers=["silver", "gold"],
                    confidence=0.93,
                )
            elif "quarantine_ratio" in failed_names or "critical_null_rate" in failed_names:
                has_nulls = "critical_null_rate" in failed_names
                has_dups = "duplicate_order_rate" in failed_names
                if has_nulls and has_dups:
                    category = "NULL_SPIKE_AND_DUPLICATES"
                    cause = (
                        "Upstream feed delivered high null rates in critical identifiers alongside "
                        "duplicate order_ids, triggering Silver quarantine and quality gate failure."
                    )
                elif has_nulls:
                    category = "CRITICAL_NULL_SPIKE"
                    cause = (
                        "Critical identifiers or numeric fields exceeded maximum allowed null rate, "
                        "routing affected records to Silver quarantine."
                    )
                else:
                    category = "CORRUPT_OR_INVALID_RECORDS"
                    cause = (
                        "High ratio of records failed Silver business/type validation (negative "
                        "prices/quantities, invalid timestamps, or unknown statuses)."
                    )
                evidence = [
                    f"bronze={bronze_rows}, valid_silver={silver_rows}, quarantined={quarantined}, duplicates={duplicates}",
                    f"null_rates={quality_info.get('null_rates')}",
                ]
                if q_reasons:
                    evidence.append(f"top_quarantine_reasons={q_reasons}")
                diag = FailureDiagnosis(
                    run_id=resolved_run_id,
                    status_observed=status,
                    failure_category=category,
                    severity="HIGH",
                    root_cause=cause,
                    evidence=evidence,
                    affected_layers=["silver", "quarantine", "gold"],
                    confidence=0.94,
                )
            else:
                diag = FailureDiagnosis(
                    run_id=resolved_run_id,
                    status_observed=status,
                    failure_category="QUALITY_GATE_VIOLATION",
                    severity="HIGH",
                    root_cause=str(quality_info.get("summary") or error_msg),
                    evidence=[c["details"] for c in failed_checks] or [error_msg],
                    affected_layers=["silver", "gold"],
                    confidence=0.88,
                )

            self._record_trace("identify_failure_reason", resolved_run_id, diag.failure_category)
            return diag

        diag = FailureDiagnosis(
            run_id=resolved_run_id,
            status_observed=status,
            failure_category="PIPELINE_EXECUTION_ERROR",
            severity="CRITICAL",
            root_cause=f"Pipeline failed with {error_type or 'Error'}: {error_msg}",
            evidence=[
                f"error_type={error_type}",
                f"error_message={error_msg}",
                *status_info.get("log_tail", [])[-5:],
            ],
            affected_layers=["bronze", "silver", "gold"],
            confidence=0.85,
        )
        self._record_trace("identify_failure_reason", resolved_run_id, diag.failure_category)
        return diag

    def generate_sql_validation(
        self,
        run_id: str | None = None,
        database_name: str | None = None,
    ) -> SQLValidationPack:
        """Generate read-only Amazon Athena SQL queries tailored to the run and diagnosis."""
        status_info = self.inspect_pipeline_status(run_id=run_id)
        resolved_run_id = str(status_info.get("run_id") or (run_id or "latest"))
        batch_id = str(status_info.get("batch_id") or resolved_run_id)
        db = _validate_identifier(
            database_name or self.settings.aws.glue_database, "database_name"
        )
        assert db is not None

        diagnosis = self.identify_failure_reason(run_id=resolved_run_id)

        queries: dict[str, str] = {
            "row_count_reconciliation": (
                f"SELECT 'bronze' AS layer, COUNT(*) AS row_count "
                f"FROM {db}.bronze_orders WHERE _batch_id = '{batch_id}'\n"
                f"UNION ALL\n"
                f"SELECT 'silver' AS layer, COUNT(*) AS row_count "
                f"FROM {db}.silver_orders WHERE _batch_id = '{batch_id}'\n"
                f"UNION ALL\n"
                f"SELECT 'quarantine' AS layer, COUNT(*) AS row_count "
                f"FROM {db}.silver_quarantine WHERE _batch_id = '{batch_id}';"
            ),
            "null_audit_check": (
                f"SELECT\n"
                f"  COUNT(*) AS total_rows,\n"
                f"  SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) AS null_order_id,\n"
                f"  SUM(CASE WHEN customer_id IS NULL OR trim(customer_id) = '' THEN 1 ELSE 0 END) AS null_customer_id,\n"
                f"  SUM(CASE WHEN unit_price IS NULL OR trim(unit_price) = '' THEN 1 ELSE 0 END) AS null_unit_price,\n"
                f"  SUM(CASE WHEN order_timestamp IS NULL OR trim(order_timestamp) = '' THEN 1 ELSE 0 END) AS null_timestamp\n"
                f"FROM {db}.bronze_orders\n"
                f"WHERE _batch_id = '{batch_id}';"
            ),
            "duplicate_order_check": (
                f"SELECT order_id, COUNT(*) AS occurrence_count\n"
                f"FROM {db}.bronze_orders\n"
                f"WHERE _batch_id = '{batch_id}'\n"
                f"GROUP BY order_id\n"
                f"HAVING COUNT(*) > 1\n"
                f"ORDER BY occurrence_count DESC\n"
                f"LIMIT 25;"
            ),
            "quarantine_reason_breakdown": (
                f"SELECT _quarantine_reason, COUNT(*) AS affected_rows\n"
                f"FROM {db}.silver_quarantine\n"
                f"WHERE _batch_id = '{batch_id}'\n"
                f"GROUP BY _quarantine_reason\n"
                f"ORDER BY affected_rows DESC;"
            ),
            "gold_revenue_reconciliation": (
                f"SELECT\n"
                f"  ROUND(SUM(s.net_amount), 2) AS silver_net_revenue,\n"
                f"  (SELECT ROUND(SUM(g.net_revenue), 2) FROM {db}.gold_daily_revenue g) AS gold_net_revenue\n"
                f"FROM {db}.silver_orders s\n"
                f"WHERE s._batch_id = '{batch_id}';"
            ),
        }

        if diagnosis.failure_category == "SCHEMA_DRIFT":
            queries["schema_inspection"] = f"SHOW COLUMNS IN {db}.bronze_orders;"
        elif diagnosis.failure_category == "CORRUPT_OR_INVALID_RECORDS":
            queries["numeric_domain_check"] = (
                f"SELECT order_id, quantity, unit_price, order_status, order_timestamp\n"
                f"FROM {db}.bronze_orders\n"
                f"WHERE _batch_id = '{batch_id}'\n"
                f"  AND (try_cast(unit_price AS DOUBLE) <= 0 OR try_cast(quantity AS INT) <= 0)\n"
                f"LIMIT 50;"
            )

        all_read_only = all(validate_read_only_sql(sql) for sql in queries.values())
        pack = SQLValidationPack(
            run_id=resolved_run_id,
            database_name=db,
            dialect="athena",
            queries=queries,
            read_only_verified=all_read_only,
        )
        self._record_trace("generate_sql_validation", resolved_run_id, f"OK:{len(queries)}_queries")
        return pack

    def recommend_recovery(self, run_id: str | None = None) -> RecoveryRecommendation:
        """Produce a safe, human-approved recovery plan (never executed automatically)."""
        diagnosis = self.identify_failure_reason(run_id=run_id)
        resolved_run_id = diagnosis.run_id
        category = diagnosis.failure_category
        rec_id = f"rec_{resolved_run_id}_{uuid.uuid4().hex[:6]}"

        actions: list[RecoveryAction] = []
        if category == "HEALTHY_RUN":
            actions.append(
                RecoveryAction(
                    action_id="act_verify_gold",
                    title="No remediation required — verify Gold tables",
                    description="Run read-only Athena reconciliation queries to confirm Gold totals match Silver.",
                    command_or_step="Execute SQLValidationPack['gold_revenue_reconciliation'] in Athena.",
                    risk_level="LOW",
                    destructive=False,
                    rollback_plan="Read-only verification; no state changes made.",
                )
            )
        elif category == "SCHEMA_DRIFT":
            actions.extend(
                [
                    RecoveryAction(
                        action_id="act_contact_producer",
                        title="Confirm upstream schema change or map drifted columns",
                        description=(
                            "Verify with upstream producer whether renamed columns (e.g., item_cost_usd -> unit_price) "
                            "and dropped columns (e.g., customer_id) are intentional or an accidental export regression."
                        ),
                        command_or_step="Inspect raw CSV header and compare against EXPECTED_RAW_COLUMNS in schemas.py.",
                        risk_level="LOW",
                        destructive=False,
                        rollback_plan="Inspection only; Bronze raw data remains preserved.",
                    ),
                    RecoveryAction(
                        action_id="act_reprocess_schema_map",
                        title="Apply explicit Bronze->Silver schema mapping after upstream fix",
                        description=(
                            "Once corrected CSV arrives or an approved schema evolution PR adds column aliases, "
                            "re-run the batch into a new Silver partition without deleting existing Bronze audit logs."
                        ),
                        command_or_step=(
                            "python scripts/run_local_pipeline.py --input-csv <corrected_csv_path>"
                        ),
                        risk_level="MEDIUM",
                        destructive=False,
                        rollback_plan="Each batch_id writes to an isolated directory; remove new batch_id folder to revert.",
                    ),
                ]
            )
        elif category in ("CRITICAL_NULL_SPIKE", "NULL_SPIKE_AND_DUPLICATES", "EXCESSIVE_DUPLICATES"):
            actions.extend(
                [
                    RecoveryAction(
                        action_id="act_audit_quarantine",
                        title="Audit quarantined & duplicate records before promotion",
                        description=(
                            "Review silver/quarantine Parquet and Athena duplicate_order_check output "
                            "to determine whether missing fields came from a truncated upstream extract."
                        ),
                        command_or_step="Run SQLValidationPack['null_audit_check'] and inspect quarantine Parquet.",
                        risk_level="LOW",
                        destructive=False,
                        rollback_plan="Read-only inspection; Silver and Gold remain protected.",
                    ),
                    RecoveryAction(
                        action_id="act_backfill_clean_extract",
                        title="Request clean upstream re-extract and re-run Silver/Gold",
                        description=(
                            "Backfill the affected batch from a validated source snapshot so Gold metrics "
                            "are not skewed by incomplete customer_id/unit_price rows."
                        ),
                        command_or_step=(
                            "python scripts/run_local_pipeline.py --input-csv <backfilled_csv_path>"
                        ),
                        risk_level="MEDIUM",
                        destructive=False,
                        rollback_plan="Prior Gold outputs remain untouched because the quality gate halted promotion.",
                    ),
                ]
            )
        elif category == "CORRUPT_OR_INVALID_RECORDS":
            actions.extend(
                [
                    RecoveryAction(
                        action_id="act_inspect_quarantine",
                        title="Review quarantined rows for price/quantity/date corruption",
                        description=(
                            "Check _quarantine_reason counts in silver/quarantine to isolate whether "
                            "corrupt rows stem from one upstream POS/channel or currency format change."
                        ),
                        command_or_step="Run SQLValidationPack['quarantine_reason_breakdown'] and numeric_domain_check.",
                        risk_level="LOW",
                        destructive=False,
                        rollback_plan="Read-only audit.",
                    ),
                    RecoveryAction(
                        action_id="act_conditional_promote",
                        title="Optional: Promote valid Silver rows after human sign-off",
                        description=(
                            "If quarantined rows are confirmed to be isolated test/refund anomalies and valid "
                            "orders are clean, re-run with --allow-quality-warnings after raising/documenting waiver."
                        ),
                        command_or_step=(
                            f"python scripts/run_local_pipeline.py --input-csv <source_csv> --allow-quality-warnings"
                        ),
                        risk_level="HIGH",
                        destructive=False,
                        rollback_plan="Delete the newly created batch_id under data/gold/ and re-run strict gate.",
                    ),
                ]
            )
        elif category == "EMPTY_INPUT_DATASET":
            actions.append(
                RecoveryAction(
                    action_id="act_check_upstream_export",
                    title="Verify upstream ingestion drop and trigger delayed batch retry",
                    description=(
                        "Confirm whether the upstream S3/CSV drop arrived late or exported zero rows "
                        "before scheduling a clean retry."
                    ),
                    command_or_step="Verify source file size > 0 bytes, then re-run scripts/run_local_pipeline.py.",
                    risk_level="LOW",
                    destructive=False,
                    rollback_plan="Gold layer was not updated on empty batch; no rollback needed.",
                )
            )
        else:
            actions.append(
                RecoveryAction(
                    action_id="act_triage_logs",
                    title="Inspect pipeline logs and environment configuration",
                    description=(
                        "Review logs/<run_id>.log, Spark settings, and storage permissions before retrying."
                    ),
                    command_or_step=f"Inspect logs/{resolved_run_id}.log and verify .env settings.",
                    risk_level="MEDIUM",
                    destructive=False,
                    rollback_plan="No automated changes were performed.",
                )
            )

        recommendation = RecoveryRecommendation(
            recommendation_id=rec_id,
            run_id=resolved_run_id,
            diagnosis_summary=diagnosis.root_cause,
            failure_category=category,
            actions=actions,
            requires_human_approval=True,
            approval_status="PENDING_APPROVAL",
            auto_executed=False,
        )
        self.approval_store.save_recommendation(recommendation)
        self._record_trace("recommend_recovery", resolved_run_id, f"PENDING_APPROVAL:{rec_id}")
        return recommendation
