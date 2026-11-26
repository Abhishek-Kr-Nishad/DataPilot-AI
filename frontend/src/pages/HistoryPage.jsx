import React, { useEffect, useState } from "react";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { EmptyState, ErrorBanner, LoadingPanel } from "../components/UiPrimitives";

export function HistoryPage({ selectedRunId, onSelectRun, onNavigate, refreshToken }) {
  const [runs, setRuns] = useState([]);
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api.listRuns(100, statusFilter);
      setRuns(data || []);
    } catch (err) {
      setError(err.message || "Failed loading execution history.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, [statusFilter, refreshToken]);

  const filtered = runs.filter((r) => {
    if (!query.trim()) return true;
    const q = query.toLowerCase();
    return (
      r.run_id.toLowerCase().includes(q) ||
      r.scenario.toLowerCase().includes(q) ||
      (r.error_message || "").toLowerCase().includes(q)
    );
  });

  if (loading) return <LoadingPanel message="Loading pipeline execution history..." />;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-bold text-white">Pipeline Execution History</h2>
          <p className="text-sm text-slate-400">
            Audit trail of all PySpark runs, execution modes, durations, and failure states
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <input
            type="text"
            placeholder="Search run_id or scenario..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-100"
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-100"
          >
            <option value="ALL">All Statuses</option>
            <option value="SUCCESS">SUCCESS</option>
            <option value="QUALITY_FAILED">QUALITY_FAILED</option>
            <option value="FAILED">FAILED</option>
          </select>
        </div>
      </div>

      <ErrorBanner error={error} onRetry={load} />

      {filtered.length === 0 ? (
        <EmptyState
          title="No Matching Pipeline Runs"
          description="Try clearing the status filter or execute a new batch in Pipeline Management."
        />
      ) : (
        <div className="overflow-x-auto rounded-xl border border-slate-800 bg-slate-900/70">
          <table className="w-full text-left text-xs">
            <thead className="border-b border-slate-800 bg-slate-950/60 text-slate-400">
              <tr>
                <th className="px-4 py-3">Run ID</th>
                <th className="px-4 py-3">Scenario</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Mode</th>
                <th className="px-4 py-3">Bronze / Silver / Q</th>
                <th className="px-4 py-3">Duration</th>
                <th className="px-4 py-3">Error / Notes</th>
                <th className="px-4 py-3">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/70">
              {filtered.map((r) => {
                const bronze = r.layers?.bronze?.row_count ?? 0;
                const silver = r.layers?.silver?.row_count ?? 0;
                const q = r.layers?.quarantine?.row_count ?? 0;
                const isSelected = selectedRunId === r.run_id;
                return (
                  <tr
                    key={r.run_id}
                    className={isSelected ? "bg-sky-950/30" : "hover:bg-slate-800/40"}
                  >
                    <td className="px-4 py-2.5 font-mono text-sky-300">{r.run_id}</td>
                    <td className="px-4 py-2.5 text-slate-300">{r.scenario}</td>
                    <td className="px-4 py-2.5"><StatusBadge value={r.status} /></td>
                    <td className="px-4 py-2.5 uppercase text-slate-400">
                      {r.effective_mode} {r.aws_synced ? "(S3)" : ""}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-slate-300">
                      {bronze} / {silver} / {q}
                    </td>
                    <td className="px-4 py-2.5 text-slate-400">{r.duration_seconds ?? 0}s</td>
                    <td className="max-w-xs truncate px-4 py-2.5 text-slate-400">
                      {r.error_message || "—"}
                    </td>
                    <td className="px-4 py-2.5 space-x-2 whitespace-nowrap">
                      <button
                        onClick={() => {
                          onSelectRun(r.run_id);
                          onNavigate("quality");
                        }}
                        className="text-sky-400 hover:underline"
                      >
                        Quality
                      </button>
                      <button
                        onClick={() => {
                          onSelectRun(r.run_id);
                          onNavigate("agent");
                        }}
                        className="text-amber-300 hover:underline"
                      >
                        Diagnose
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
