"""Unit and integration tests for the DataPilot AI FastAPI service (/api/v1/*)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pydantic import SecretStr
import pytest
from pyspark.sql import SparkSession

from datapilot.api.main import create_app
from datapilot.core.config import load_settings
from datapilot.data_generator import generate_ecommerce_dataset
from datapilot.schemas.domain import (
    LayerMetrics,
    PipelineRunRecord,
    QualityCheckResult,
    QualityReport,
)


def _make_client(
    tmp_path: Path,
    spark: SparkSession | None = None,
    gemini_client=None,
) -> TestClient:
    repo_root = Path(__file__).resolve().parents[1]
    settings = load_settings(project_root=repo_root)
    data_dir = tmp_path / "data"
    settings.local.data_dir = data_dir
    settings.local.raw_dir = data_dir / "raw"
    settings.local.bronze_dir = data_dir / "bronze"
    settings.local.silver_dir = data_dir / "silver"
    settings.local.quarantine_dir = data_dir / "silver" / "quarantine"
    settings.local.gold_dir = data_dir / "gold"
    settings.local.reports_dir = data_dir / "reports"
    settings.local.approvals_dir = data_dir / "approvals"
    settings.local.logs_dir = tmp_path / "logs"
    settings.metadata.database_url = f"sqlite:///{(tmp_path / 'test_api.db').as_posix()}"
    settings.api_key = None
    settings.require_auth = False
    settings.ensure_directories()

    app = create_app(settings=settings, spark=spark, gemini_client=gemini_client)
    return TestClient(app)


def test_health_openapi_and_aws_endpoints(tmp_path: Path) -> None:
    client = _make_client(tmp_path)
    health = client.get("/health")
    assert health.status_code == 200
    body = health.json()
    assert body["status"] == "ok"
    assert body["effective_mode"] == "local"
    assert body["database_backend"] == "sqlite"
    assert body["aws"]["ready_for_aws"] is False

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    paths = openapi.json()["paths"]
    for expected in (
        "/health",
        "/api/v1/pipeline/run",
        "/api/v1/pipeline/status",
        "/api/v1/pipeline/runs",
        "/api/v1/quality/report",
        "/api/v1/analytics/summary",
        "/api/v1/agent/diagnose",
        "/api/v1/agent/recommend",
    ):
        assert expected in paths

    aws_status = client.get("/api/v1/aws/status")
    assert aws_status.status_code == 200
    assert aws_status.json()["aws"]["ready_for_aws"] is False

    athena = client.get("/api/v1/aws/athena-sql")
    assert athena.status_code == 200
    assert len(athena.json()["scripts"]) == 5
    assert len(athena.json()["ddl_statements"]) == 5


def test_agent_recommend_and_approval_endpoints_without_spark(tmp_path: Path) -> None:
    fake_gemini = SimpleNamespace(
        models=SimpleNamespace(
            generate_content=lambda **kwargs: SimpleNamespace(
                text="Gemini diagnostic insight: customer_id missing due to upstream drift."
            )
        )
    )
    client = _make_client(tmp_path, gemini_client=fake_gemini)
    container = client.app.state.container
    store = container.metadata_store

    store.save_run(
        PipelineRunRecord(
            run_id="run_api_01",
            batch_id="run_api_01",
            status="FAILED",
            requested_mode="local",
            effective_mode="local",
            source_file="orders.csv",
            scenario="schema_drift",
            layers={
                "bronze": LayerMetrics(
                    layer="bronze", path=str(tmp_path), row_count=40, column_count=17
                )
            },
            error_type="SchemaDriftError",
            error_message="Missing customer_id",
        )
    )
    store.save_quality_report(
        QualityReport(
            run_id="run_api_01",
            dataset_name="orders.csv",
            overall_passed=False,
            quality_score=0.0,
            total_bronze_rows=40,
            valid_silver_rows=0,
            quarantined_rows=0,
            duplicate_rows_removed=0,
            schema_valid=False,
            missing_columns=["customer_id"],
            checks=[
                QualityCheckResult(
                    check_name="schema_contract",
                    passed=False,
                    severity="ERROR",
                    metric_value=1,
                    threshold="0 missing",
                    affected_rows=40,
                    details="Missing customer_id",
                )
            ],
            summary="FAILED schema contract",
        )
    )

    status_resp = client.get("/api/v1/pipeline/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["total_runs"] == 1
    assert status_resp.json()["failed_count"] == 1

    quality_resp = client.get("/api/v1/quality/report")
    assert quality_resp.status_code == 200
    assert quality_resp.json()["run_id"] == "run_api_01"

    diag = client.post("/api/v1/agent/diagnose", json={"run_id": "run_api_01", "mode": "gemini"})
    assert diag.status_code == 200
    diag_data = diag.json()
    assert diag_data["llm_provider"] == "gemini"
    assert diag_data["diagnosis"]["failure_category"] == "SCHEMA_DRIFT"

    rec_resp = client.post("/api/v1/agent/recommend", json={"run_id": "run_api_01", "mode": "mock"})
    assert rec_resp.status_code == 200
    rec_id = rec_resp.json()["recommendation_id"]
    assert rec_resp.json()["approval_status"] == "PENDING_APPROVAL"
    assert rec_resp.json()["auto_executed"] is False

    sql_resp = client.get("/api/v1/agent/tools/sql?run_id=run_api_01")
    assert sql_resp.status_code == 200
    assert sql_resp.json()["read_only_verified"] is True

    bad_approve = client.post(
        f"/api/v1/agent/approvals/{rec_id}/approve",
        json={"reviewer": "   ", "notes": "ok"},
    )
    assert bad_approve.status_code == 422

    ok_approve = client.post(
        f"/api/v1/agent/approvals/{rec_id}/approve",
        json={"reviewer": "abhishek.nishad", "notes": "Verified schema fix"},
    )
    assert ok_approve.status_code == 200
    assert ok_approve.json()["approval_status"] == "APPROVED"

    dup = client.post(
        f"/api/v1/agent/approvals/{rec_id}/reject",
        json={"reviewer": "other", "notes": "late"},
    )
    assert dup.status_code == 409

    audit = client.get("/api/v1/agent/approvals/audit")
    assert audit.status_code == 200
    assert len(audit.json()) == 1

    sync_resp = client.post("/api/v1/aws/sync/run_api_01")
    assert sync_resp.status_code == 400


def test_api_validation_and_empty_analytics(tmp_path: Path) -> None:
    client = _make_client(tmp_path)
    assert client.get("/api/v1/pipeline/runs/nonexistent_run").status_code == 404
    assert client.get("/api/v1/pipeline/runs/bad;id").status_code == 422

    # Empty DB state must return clean 200 empty structures (never HTTP 500)
    empty_status = client.get("/api/v1/pipeline/status")
    assert empty_status.status_code == 200
    assert empty_status.json()["has_runs"] is False
    assert client.get("/api/v1/pipeline/runs").json() == []
    assert client.get("/api/v1/quality/report").status_code == 404

    empty_analytics = client.get("/api/v1/analytics/summary")
    assert empty_analytics.status_code == 200
    assert empty_analytics.json()["has_data"] is False

    # Corrupted DB payload row must not crash /runs or /status with HTTP 500
    from datapilot.models.orm import PipelineRunORM

    container = client.app.state.container
    with container.db_manager.session_scope() as session:
        session.add(
            PipelineRunORM(
                run_id="corrupt_row",
                batch_id="corrupt_row",
                started_at="2026-01-01T00:00:00Z",
                status="FAILED",
                requested_mode="local",
                effective_mode="local",
                aws_synced=False,
                scenario="bad",
                payload_json="{not-valid-json",
            )
        )
    assert client.get("/api/v1/pipeline/runs").status_code == 200
    assert client.get("/api/v1/pipeline/status").status_code == 200

    bad_upload = client.post(
        "/api/v1/pipeline/upload",
        files={"file": ("payload.txt", b"not csv", "text/plain")},
    )
    assert bad_upload.status_code == 400


def test_api_end_to_end_pipeline_and_analytics_summary(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    client = _make_client(tmp_path, spark=spark_session)

    run_resp = client.post(
        "/api/v1/pipeline/run",
        json={"scenario": "clean", "num_rows": 60, "seed": 42, "fail_on_quality_error": True},
    )
    assert run_resp.status_code == 201
    run_data = run_resp.json()
    run_id = run_data["run_record"]["run_id"]
    assert run_data["run_record"]["status"] == "SUCCESS"
    assert run_data["quality_report"]["overall_passed"] is True
    assert run_data["quality_report"]["quality_score"] == 100.0

    runs_list = client.get("/api/v1/pipeline/runs").json()
    assert len(runs_list) == 1
    assert runs_list[0]["run_id"] == run_id

    summary = client.get(f"/api/v1/analytics/summary?run_id={run_id}")
    assert summary.status_code == 200
    s_data = summary.json()
    assert s_data["has_data"] is True
    assert s_data["total_records_processed"] == 60
    assert s_data["valid_records"] == 60
    assert s_data["total_net_revenue"] > 0
    assert len(s_data["daily_revenue"]) >= 1
    assert len(s_data["category_performance"]) >= 1

    preview = client.get(f"/api/v1/pipeline/runs/{run_id}/layers/gold_daily_revenue?limit=10")
    assert preview.status_code == 200
    assert preview.json()["total_rows"] >= 1

    corrupt_csv = generate_ecommerce_dataset(
        tmp_path / "upload_corrupt.csv", num_rows=40, seed=7, scenario="corrupt_prices"
    )
    upload_resp = client.post(
        "/api/v1/pipeline/upload",
        files={"file": ("upload_corrupt.csv", corrupt_csv.read_bytes(), "text/csv")},
        data={"fail_on_quality_error": "true"},
    )
    assert upload_resp.status_code == 201
    corrupt_run_id = upload_resp.json()["run_record"]["run_id"]
    assert upload_resp.json()["run_record"]["status"] == "QUALITY_FAILED"

    diag = client.post(
        "/api/v1/agent/diagnose",
        json={"run_id": corrupt_run_id, "mode": "mock"},
    ).json()
    assert diag["diagnosis"]["failure_category"] == "CORRUPT_OR_INVALID_RECORDS"


def test_api_security_auth_cors_upload_limit_and_error_masking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1. Verify DATAPILOT_CORS_ORIGINS is respected when create_app(settings=None) runs
    monkeypatch.setenv("DATAPILOT_DATA_DIR", str(tmp_path / "env_data"))
    monkeypatch.setenv("DATAPILOT_CORS_ORIGINS", "https://analytics.example.com")
    monkeypatch.delenv("DATAPILOT_API_KEY", raising=False)
    monkeypatch.setenv("DATAPILOT_REQUIRE_AUTH", "false")
    env_app = create_app()
    with TestClient(env_app) as env_client:
        preflight = env_client.options(
            "/health",
            headers={
                "Origin": "https://analytics.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert preflight.status_code == 200
        assert preflight.headers.get("access-control-allow-origin") == "https://analytics.example.com"

    # 2. Verify API key auth blocks unauthenticated /api/v1/* calls while /health remains open
    repo_root = Path(__file__).resolve().parents[1]
    auth_settings = load_settings(project_root=repo_root)
    auth_settings.local.data_dir = tmp_path / "auth_data"
    auth_settings.local.raw_dir = auth_settings.local.data_dir / "raw"
    auth_settings.local.bronze_dir = auth_settings.local.data_dir / "bronze"
    auth_settings.local.silver_dir = auth_settings.local.data_dir / "silver"
    auth_settings.local.quarantine_dir = auth_settings.local.data_dir / "silver" / "quarantine"
    auth_settings.local.gold_dir = auth_settings.local.data_dir / "gold"
    auth_settings.local.reports_dir = auth_settings.local.data_dir / "reports"
    auth_settings.local.approvals_dir = auth_settings.local.data_dir / "approvals"
    auth_settings.local.logs_dir = tmp_path / "auth_logs"
    auth_settings.metadata.database_url = f"sqlite:///{(tmp_path / 'auth.db').as_posix()}"
    auth_settings.api_key = SecretStr("unit-test-secret-key")
    auth_settings.max_upload_mb = 1
    auth_settings.debug = False
    auth_settings.ensure_directories()

    auth_app = create_app(settings=auth_settings)

    @auth_app.get("/api/v1/test-boom")
    def _boom():
        raise RuntimeError("sensitive postgresql://user:pw@db:5432/secret")

    auth_client = TestClient(auth_app, raise_server_exceptions=False)
    assert auth_client.get("/health").json()["auth_enabled"] is True
    assert auth_client.get("/api/v1/pipeline/runs").status_code == 401
    assert (
        auth_client.get("/api/v1/pipeline/runs", headers={"X-API-Key": "wrong"}).status_code == 401
    )
    assert (
        auth_client.get(
            "/api/v1/pipeline/runs", headers={"X-API-Key": "unit-test-secret-key"}
        ).status_code
        == 200
    )
    assert (
        auth_client.get(
            "/api/v1/pipeline/runs",
            headers={"Authorization": "Bearer unit-test-secret-key"},
        ).status_code
        == 200
    )

    # 3. Verify HTTP 500 error masking hides internal exception details
    boom_resp = auth_client.get(
        "/api/v1/test-boom", headers={"X-API-Key": "unit-test-secret-key"}
    )
    assert boom_resp.status_code == 500
    assert "Internal server error" in boom_resp.json()["detail"]
    assert "secret" not in boom_resp.text
    assert "RuntimeError" not in boom_resp.text

    # 4. Verify bounded streaming CSV upload returns 413 when exceeding max_upload_mb
    oversized = b"order_id,unit_price\n" + (b"ORD-1,10.00\n" * 100_000)
    too_big = auth_client.post(
        "/api/v1/pipeline/upload",
        headers={"X-API-Key": "unit-test-secret-key"},
        files={"file": ("big.csv", oversized, "text/csv")},
    )
    assert too_big.status_code == 413

    # 5. Verify fail-closed 503 when require_auth=True without api_key
    auth_settings.api_key = None
    auth_settings.require_auth = True
    fail_closed_client = TestClient(create_app(settings=auth_settings))
    assert fail_closed_client.get("/api/v1/pipeline/runs").status_code == 503
