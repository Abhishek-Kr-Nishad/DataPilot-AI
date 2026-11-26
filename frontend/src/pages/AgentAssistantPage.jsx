import React, { useEffect, useState } from "react";
import { Bot, Check, ShieldAlert, Terminal, X } from "lucide-react";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState, ErrorBanner, LoadingPanel } from "../components/UiPrimitives";

export function AgentAssistantPage({ selectedRunId, refreshToken }) {
  const [runs, setRuns] = useState([]);
  const [targetRunId, setTargetRunId] = useState(selectedRunId || "");
  const [agentMode, setAgentMode] = useState("auto");
  const [diagnosisReport, setDiagnosisReport] = useState(null);
  const [recommendations, setRecommendations] = useState([]);
  const [auditEvents, setAuditEvents] = useState([]);
  const [reviewer, setReviewer] = useState("abhishek.nishad");
  const [notes, setNotes] = useState("");
  const [loading, setLoading] = useState(true);
  const [diagnosing, setDiagnosing] = useState(false);
  const [error, setError] = useState("");

  const loadState = async (runOverride) => {
    setLoading(true);
    setError("");
    try {
      const [runList, recs, audit] = await Promise.all([
        api.listRuns(30),
        api.listRecommendations(),
        api.listApprovalAudit()
      ]);
      setRuns(runList || []);
      setRecommendations(recs || []);
      setAuditEvents(audit || []);
      const resolved = runOverride || selectedRunId || runList?.[0]?.run_id || "";
      setTargetRunId(resolved);
    } catch (err) {
      setError(err.message || "Failed loading AI agent workspace.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadState(selectedRunId);
  }, [selectedRunId, refreshToken]);

  const handleDiagnose = async () => {
    if (!targetRunId) return;
    setDiagnosing(true);
    setError("");
    try {
      const report = await api.diagnoseRun(targetRunId, agentMode);
      setDiagnosisReport(report);
      const [recs, audit] = await Promise.all([
        api.listRecommendations(),
        api.listApprovalAudit()
      ]);
      setRecommendations(recs || []);
      setAuditEvents(audit || []);
    } catch (err) {
      setError(err.message || "Diagnosis failed.");
    } finally {
      setDiagnosing(false);
    }
  };

  const handleDecision = async (recId, action) => {
    setError("");
    try {
      if (action === "approve") {
        await api.approveRecommendation(recId, reviewer, notes);
      } else {
        await api.rejectRecommendation(recId, reviewer, notes);
      }
      setNotes("");
      await loadState(targetRunId);
    } catch (err) {
      setError(err.message || "Failed to record decision.");
    }
  };

  if (loading) return <LoadingPanel message="Loading Gemini diagnostic assistant..." />;

  if (runs.length === 0) {
    return (
      <EmptyState
        title="No Pipeline Runs to Diagnose"
        description="Execute a clean or failure-injected pipeline batch first, then use the AI Diagnostic Agent to inspect root causes and generate Athena validation queries."
      />
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-bold text-white">AI Agent Assistant (Gemini + Tool Calling)</h2>
          <p className="text-sm text-slate-400">
            Read-only diagnostics, Athena validation SQL, and human-approved recovery plans
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <select
            value={targetRunId}
            onChange={(e) => setTargetRunId(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-xs text-slate-100"
          >
            {runs.map((r) => (
              <option key={r.run_id} value={r.run_id}>
                {r.run_id} ({r.status} · {r.scenario})
              </option>
            ))}
          </select>
          <select
            value={agentMode}
            onChange={(e) => setAgentMode(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-xs text-slate-100"
          >
            <option value="auto">Mode: auto (Gemini if key set)</option>
            <option value="mock">Mode: mock (deterministic)</option>
            <option value="gemini">Mode: gemini (strict)</option>
          </select>
          <button
            onClick={handleDiagnose}
            disabled={diagnosing || !targetRunId}
            className="inline-flex items-center gap-2 rounded-lg bg-sky-600 px-4 py-2 text-xs font-semibold text-white hover:bg-sky-500 disabled:opacity-50"
          >
            <Bot className="h-4 w-4" />
            {diagnosing ? "Diagnosing..." : "Run Diagnostic Agent"}
          </button>
        </div>
      </div>

      <ErrorBanner error={error} />

      <div className="flex items-center gap-2 rounded-xl border border-amber-500/30 bg-amber-950/25 px-4 py-2.5 text-xs text-amber-200">
        <ShieldAlert className="h-4 w-4 shrink-0" />
        Safety Guarantee: All SQL queries are validated read-only and recovery actions require explicit human approval before execution.
      </div>

      {diagnosisReport && (
        <div className="space-y-5 rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <span className="rounded-md bg-sky-500/20 px-2.5 py-1 font-mono text-xs font-semibold text-sky-300">
                {diagnosisReport.diagnosis.failure_category}
              </span>
              <span className="text-xs text-slate-400">
                Severity: <strong className="text-slate-100">{diagnosisReport.diagnosis.severity}</strong> ·
                Confidence:{" "}
                <strong className="text-slate-100">
                  {Math.round(diagnosisReport.diagnosis.confidence * 100)}%
                </strong>
              </span>
            </div>
            <span className="rounded-full border border-slate-700 px-2.5 py-0.5 text-xs text-slate-300">
              Provider: {diagnosisReport.llm_provider}
              {diagnosisReport.llm_model ? ` (${diagnosisReport.llm_model})` : ""}
            </span>
          </div>

          <p className="text-sm text-slate-200">{diagnosisReport.narrative_summary}</p>

          <div>
            <h4 className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-slate-400">
              Observed Evidence
            </h4>
            <ul className="list-disc space-y-1 pl-5 text-xs text-slate-300">
              {diagnosisReport.diagnosis.evidence.map((ev, i) => (
                <li key={i} className="font-mono">{ev}</li>
              ))}
            </ul>
          </div>

          <div>
            <h4 className="mb-2 flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-slate-400">
              <Terminal className="h-3.5 w-3.5" />
              Read-Only Amazon Athena Validation SQL (Verified: {String(diagnosisReport.sql_validation.read_only_verified)})
            </h4>
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
              {Object.entries(diagnosisReport.sql_validation.queries).map(([name, sql]) => (
                <div key={name} className="rounded-lg border border-slate-800 bg-slate-950 p-3">
                  <div className="mb-1 text-xs font-semibold text-sky-300">{name}</div>
                  <pre className="overflow-x-auto text-[11px] text-slate-300">{sql}</pre>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-white">Human-in-the-Loop Approval Queue</h3>
          <div className="flex flex-wrap items-center gap-2">
            <input
              type="text"
              placeholder="Reviewer"
              value={reviewer}
              onChange={(e) => setReviewer(e.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-1.5 text-xs text-slate-100"
            />
            <input
              type="text"
              placeholder="Approval / rejection notes..."
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              className="w-60 rounded-lg border border-slate-700 bg-slate-950 px-3 py-1.5 text-xs text-slate-100"
            />
          </div>
        </div>

        {recommendations.length === 0 ? (
          <p className="text-xs text-slate-400">No recovery proposals created yet.</p>
        ) : (
          <div className="space-y-3">
            {recommendations.map((rec) => (
              <div
                key={rec.recommendation_id}
                className="rounded-lg border border-slate-800 bg-slate-950/60 p-4 space-y-2"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="text-xs">
                    <span className="font-mono text-sky-300">{rec.recommendation_id}</span> · Run{" "}
                    <span className="font-mono text-slate-300">{rec.run_id}</span> ·{" "}
                    <span className="text-slate-400">{rec.failure_category}</span>
                  </div>
                  <StatusBadge value={rec.approval_status} />
                </div>
                <p className="text-xs text-slate-300">{rec.diagnosis_summary}</p>
                <ul className="space-y-1 text-xs text-slate-400">
                  {rec.actions.map((act) => (
                    <li key={act.action_id}>
                      <strong className="text-slate-200">[{act.risk_level}] {act.title}:</strong>{" "}
                      {act.description}{" "}
                      <code className="text-sky-300">{act.command_or_step}</code>
                    </li>
                  ))}
                </ul>
                {rec.approval_status === "PENDING_APPROVAL" ? (
                  <div className="flex gap-2 pt-2">
                    <button
                      onClick={() => handleDecision(rec.recommendation_id, "approve")}
                      className="inline-flex items-center gap-1 rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-emerald-500"
                    >
                      <Check className="h-3.5 w-3.5" /> Approve
                    </button>
                    <button
                      onClick={() => handleDecision(rec.recommendation_id, "reject")}
                      className="inline-flex items-center gap-1 rounded-lg bg-rose-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-rose-500"
                    >
                      <X className="h-3.5 w-3.5" /> Reject
                    </button>
                  </div>
                ) : (
                  <div className="text-xs text-slate-400">
                    Reviewed by <strong className="text-slate-200">{rec.approved_by}</strong> at{" "}
                    {rec.approved_at} {rec.reviewer_notes ? `— "${rec.reviewer_notes}"` : ""}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {auditEvents.length > 0 && (
          <div className="pt-2">
            <h4 className="mb-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
              Approval Audit Trail
            </h4>
            <div className="overflow-x-auto text-xs">
              <table className="w-full text-left">
                <thead className="border-b border-slate-800 text-slate-400">
                  <tr>
                    <th className="py-1.5 pr-3">Recommendation</th>
                    <th className="py-1.5 pr-3">Decision</th>
                    <th className="py-1.5 pr-3">Reviewer</th>
                    <th className="py-1.5">Timestamp</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {auditEvents.map((e, i) => (
                    <tr key={i}>
                      <td className="py-1.5 pr-3 font-mono text-slate-300">{e.recommendation_id}</td>
                      <td className="py-1.5 pr-3"><StatusBadge value={e.decision} /></td>
                      <td className="py-1.5 pr-3 text-slate-300">{e.reviewer}</td>
                      <td className="py-1.5 text-slate-400">{e.decided_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
