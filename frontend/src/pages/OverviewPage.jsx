import React, { useEffect, useState } from "react";
import {
  Activity,
  AlertTriangle,
  Bot,
  CheckCircle2,
  CopyCheck,
  Database,
  Play,
  ShieldCheck
} from "lucide-react";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState, ErrorBanner, KpiCard, LoadingPanel } from "../components/UiPrimitives";

export function OverviewPage({
  health,
  selectedRunId,
  onSelectRun,
  onNavigate,
  refreshToken,
  onPipelineExecuted
}) {
  const [statusData, setStatusData] = useState(null);
  const [summary, setSummary] = useState(null);
  const [runs, setRuns] = useState([]);
  const [latestRec, setLatestRec] = useState(null);
  const [latestDiag, setLatestDiag] = useState(null);
  const [loading, setLoading] = useState(true);
  const [busyRun, setBusyRun] = useState(false);
  const [busyDiag, setBusyDiag] = useState(false);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const [st, sum, rList] = await Promise.all([
        api.getPipelineStatus(selectedRunId),
        api.getAnalyticsSummary(selectedRunId),
        api.listRuns(8)
      ]);
      setStatusData(st);
      setSummary(sum);
      setRuns(rList || []);

      const targetRunId = selectedRunId || st?.latest_run?.run_id;
      if (targetRunId) {
        try {
          const recs = await api.listRecommendations(targetRunId);
          setLatestRec(recs && recs.length > 0 ? recs[0] : null);
        } catch {
          setLatestRec(null);
        }
      } else {
        setLatestRec(null);
      }
      setLatestDiag(null);
    } catch (err) {
      setError(err.message || "Failed to load overview metrics.");
    } finally {
      setLoading(false);
    }
  };

  const handleRunDiagnosis = async () => {
    const targetRunId = selectedRunId || statusData?.latest_run?.run_id;
    if (!targetRunId) return;
    setBusyDiag(true);
    setError("");
    try {
      const diag = await api.diagnoseRun(targetRunId, "mock");
      setLatestDiag(diag);
      setLatestRec(diag.recommendation || null);
    } catch (err) {
      setError(err.message || "Failed to run AI diagnosis.");
    } finally {
      setBusyDiag(false);
    }
  };

  useEffect(() => {
    load();
  }, [selectedRunId, refreshToken]);

  const triggerQuickClean = async () => {
    setBusyRun(true);
    setError("");
    try {
      const res = await api.runSyntheticPipeline({
        scenario: "clean",
        num_rows: 200,
        seed: 42,
        fail_on_quality_error: true
      });
      onPipelineExecuted?.(res.run_record.run_id);
      await load();
    } catch (err) {
      setError(err.message || "Pipeline execution failed.");
    } finally {
      setBusyRun(false);
    }
  };

  if (loading) return <LoadingPanel message="Loading overview dashboard..." />;

  const activeRun = statusData?.latest_run;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-bold text-white">Overview Dashboard</h2>
          <p className="text-sm text-slate-400">
            Real-time Medallion ETL telemetry, data quality score, and AI diagnostics
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={triggerQuickClean}
            disabled={busyRun}
            className="inline-flex items-center gap-2 rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-500 disabled:opacity-50"
          >
            <Play className="h-4 w-4" />
            {busyRun ? "Running PySpark..." : "Run Sample Batch"}
          </button>
        </div>
      </div>

      <ErrorBanner error={error} onRetry={load} />

      {health && (
        <div className="flex flex-wrap items-center justify-between gap-4 rounded-xl border border-slate-800 bg-slate-900/60 px-4 py-3 text-xs text-slate-300">
          <div className="flex flex-wrap items-center gap-3">
            <span>Execution Mode:</span>
            <StatusBadge value={health.effective_mode} />
            <span>Database: <strong className="text-slate-100">{health.database_backend}</strong></span>
            <span>
              Gemini AI:{" "}
              <strong className="text-slate-100">
                {health.gemini_configured ? "Configured" : "Mock / Offline Mode"}
              </strong>
            </span>
          </div>
          <div>
            AWS Cloud:{" "}
            <strong className={health.aws?.ready_for_aws ? "text-emerald-300" : "text-amber-300"}>
              {health.aws?.ready_for_aws ? "Credentials & Bucket Ready" : "Local Mode (No AWS Credentials)"}
            </strong>
          </div>
        </div>
      )}

      {error ? (
        <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 text-sm text-slate-300">
          Unable to load dashboard state from the FastAPI backend. Check that{" "}
          <code className="text-sky-300">scripts/run_api.py</code> is running on{" "}
          <code className="text-sky-300">127.0.0.1:8000</code> and click <strong>Retry</strong>{" "}
          above.
        </div>
      ) : !statusData?.has_runs ? (
        <EmptyState
          title="No Pipeline Runs Yet"
          description="Run a synthetic PySpark e-commerce batch or upload a CSV in Pipeline Management to populate Bronze, Silver, Gold, and AI Diagnostics."
          actionLabel={busyRun ? "Executing..." : "Run First Clean Pipeline"}
          onAction={triggerQuickClean}
        />
      ) : (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
            <KpiCard
              title="Total Records Processed"
              value={summary?.total_records_processed ?? 0}
              subtitle={`Run: ${activeRun?.run_id || "—"}`}
              icon={Database}
              accent="text-sky-400"
            />
            <KpiCard
              title="Valid Silver Records"
              value={summary?.valid_records ?? 0}
              subtitle="Cleaned & schema-validated"
              icon={CheckCircle2}
              accent="text-emerald-400"
            />
            <KpiCard
              title="Rejected / Quarantined"
              value={summary?.rejected_records ?? 0}
              subtitle="Isolated in silver/quarantine"
              icon={AlertTriangle}
              accent="text-amber-400"
            />
            <KpiCard
              title="Duplicate Records Removed"
              value={summary?.duplicate_records ?? 0}
              subtitle="Deduplicated on order_id"
              icon={CopyCheck}
              accent="text-indigo-400"
            />
            <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium uppercase tracking-wider text-slate-400">
                  Pipeline Execution Status
                </span>
                <Activity className="h-4 w-4 text-sky-400" />
              </div>
              <div className="mt-3">
                <StatusBadge value={activeRun?.status || "UNKNOWN"} />
              </div>
              <div className="mt-2 text-xs text-slate-400">
                Scenario: {activeRun?.scenario || "—"} · Duration: {activeRun?.duration_seconds ?? 0}s
              </div>
            </div>
            <KpiCard
              title="Data Quality Score"
              value={`${(summary?.data_quality_score ?? 0).toFixed(1)}%`}
              subtitle={`Across ${statusData?.total_runs ?? 0} total runs`}
              icon={ShieldCheck}
              accent="text-emerald-400"
            />
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="text-sm font-semibold text-white">Recent Pipeline Runs</h3>
                <button
                  onClick={() => onNavigate("history")}
                  className="text-xs font-medium text-sky-400 hover:underline"
                >
                  View All
                </button>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead className="border-b border-slate-800 text-slate-400">
                    <tr>
                      <th className="py-2 pr-3">Run ID</th>
                      <th className="py-2 pr-3">Scenario</th>
                      <th className="py-2 pr-3">Status</th>
                      <th className="py-2">Mode</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/70">
                    {runs.map((r) => (
                      <tr
                        key={r.run_id}
                        onClick={() => onSelectRun(r.run_id)}
                        className="cursor-pointer hover:bg-slate-800/40"
                      >
                        <td className="py-2 pr-3 font-mono text-sky-300">{r.run_id}</td>
                        <td className="py-2 pr-3 text-slate-300">{r.scenario}</td>
                        <td className="py-2 pr-3"><StatusBadge value={r.status} /></td>
                        <td className="py-2 uppercase text-slate-400">{r.effective_mode}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
                  <Bot className="h-4 w-4 text-sky-400" />
                  Latest AI Diagnostic Summary
                </h3>
                <button
                  onClick={() => onNavigate("agent")}
                  className="text-xs font-medium text-sky-400 hover:underline"
                >
                  Open Assistant
                </button>
              </div>
              {latestDiag ? (
                <div className="space-y-3 text-sm">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-md bg-slate-800 px-2 py-0.5 font-mono text-xs text-sky-300">
                      {latestDiag.diagnosis.failure_category}
                    </span>
                    <span className="text-xs text-slate-400">
                      Severity: <strong className="text-slate-200">{latestDiag.diagnosis.severity}</strong> ·
                      Provider: <strong className="text-slate-200">{latestDiag.llm_provider}</strong>
                    </span>
                  </div>
                  <p className="text-slate-300">{latestDiag.narrative_summary}</p>
                  <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3 text-xs text-slate-400">
                    Pending Human Approval:{" "}
                    <strong className="text-slate-200">
                      {latestDiag.recommendation.recommendation_id}
                    </strong>{" "}
                    ({latestDiag.recommendation.approval_status})
                  </div>
                </div>
              ) : latestRec ? (
                <div className="space-y-3 text-sm">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-md bg-slate-800 px-2 py-0.5 font-mono text-xs text-sky-300">
                      {latestRec.failure_category}
                    </span>
                    <StatusBadge value={latestRec.approval_status} />
                  </div>
                  <p className="text-slate-300">{latestRec.diagnosis_summary}</p>
                  <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3 text-xs text-slate-400">
                    Recommendation:{" "}
                    <strong className="text-slate-200">{latestRec.recommendation_id}</strong>{" "}
                    ({latestRec.actions?.length || 0} action steps)
                  </div>
                </div>
              ) : (
                <div className="space-y-3 text-sm">
                  <p className="text-slate-400">
                    No AI diagnosis generated for this run yet.
                  </p>
                  <button
                    onClick={handleRunDiagnosis}
                    disabled={busyDiag}
                    className="inline-flex items-center gap-2 rounded-lg border border-sky-500/40 bg-sky-600/20 px-3 py-1.5 text-xs font-semibold text-sky-300 hover:bg-sky-600/30 disabled:opacity-50"
                  >
                    <Bot className="h-3.5 w-3.5" />
                    {busyDiag ? "Running Diagnosis..." : "Run AI Diagnosis (Mock Mode)"}
                  </button>
                </div>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
