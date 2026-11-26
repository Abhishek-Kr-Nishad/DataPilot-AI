import React, { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { CheckCircle2, XCircle } from "lucide-react";
import { api } from "../api/client";
import { EmptyState, ErrorBanner, KpiCard, LoadingPanel } from "../components/UiPrimitives";

export function DataQualityPage({ selectedRunId, refreshToken }) {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api.getQualityReport(selectedRunId);
      setReport(data);
    } catch (err) {
      if (String(err.message).toLowerCase().includes("not found")) {
        setReport(null);
      } else {
        setError(err.message || "Failed to load quality report.");
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, [selectedRunId, refreshToken]);

  if (loading) return <LoadingPanel message="Loading data quality metrics..." />;

  if (!report && !error) {
    return (
      <EmptyState
        title="No Data Quality Report Available"
        description="Execute a pipeline batch first to inspect schema checks, null rates, duplicate counts, and quarantine reasons."
      />
    );
  }

  const nullRateData = Object.entries(report?.null_rates || {}).map(([col, rate]) => ({
    column: col,
    nullRatePct: Number((rate * 100).toFixed(2)),
    nullCount: report.null_counts?.[col] || 0
  }));

  const quarantineData = Object.entries(report?.quarantine_reason_counts || {}).map(
    ([reason, count]) => ({ reason, count })
  );

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-white">Data Quality Monitoring</h2>
        <p className="text-sm text-slate-400">
          Schema validation, null-rate profiling, deduplication, and quarantine audit for{" "}
          <span className="font-mono text-sky-300">{report?.run_id}</span>
        </p>
      </div>

      <ErrorBanner error={error} onRetry={load} />

      {report && (
        <>
          <div
            className={`rounded-xl border px-4 py-3 text-sm ${
              report.overall_passed
                ? "border-emerald-500/40 bg-emerald-950/30 text-emerald-200"
                : "border-amber-500/40 bg-amber-950/30 text-amber-200"
            }`}
          >
            {report.summary}
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-5">
            <KpiCard title="Quality Score" value={`${report.quality_score.toFixed(1)}%`} />
            <KpiCard title="Bronze Rows" value={report.total_bronze_rows} />
            <KpiCard title="Valid Silver" value={report.valid_silver_rows} />
            <KpiCard title="Quarantined" value={report.quarantined_rows} />
            <KpiCard title="Duplicates Removed" value={report.duplicate_rows_removed} />
          </div>

          {!report.schema_valid && (
            <div className="rounded-xl border border-rose-500/40 bg-rose-950/30 p-4 text-xs text-rose-200">
              <strong>Schema Drift Detected:</strong> Missing columns{" "}
              <code>{JSON.stringify(report.missing_columns)}</code> · Unexpected columns{" "}
              <code>{JSON.stringify(report.unexpected_columns)}</code>
            </div>
          )}

          <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
            <h3 className="mb-3 text-sm font-semibold text-white">Quality Assertions</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead className="border-b border-slate-800 text-slate-400">
                  <tr>
                    <th className="py-2 pr-3">Check</th>
                    <th className="py-2 pr-3">Result</th>
                    <th className="py-2 pr-3">Severity</th>
                    <th className="py-2 pr-3">Observed</th>
                    <th className="py-2 pr-3">Threshold</th>
                    <th className="py-2">Details</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {report.checks.map((c) => (
                    <tr key={c.check_name}>
                      <td className="py-2 pr-3 font-mono text-slate-200">{c.check_name}</td>
                      <td className="py-2 pr-3">
                        {c.passed ? (
                          <span className="inline-flex items-center gap-1 text-emerald-400">
                            <CheckCircle2 className="h-3.5 w-3.5" /> PASS
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 text-rose-400">
                            <XCircle className="h-3.5 w-3.5" /> FAIL
                          </span>
                        )}
                      </td>
                      <td className="py-2 pr-3 text-slate-400">{c.severity}</td>
                      <td className="py-2 pr-3 text-slate-200">{String(c.metric_value)}</td>
                      <td className="py-2 pr-3 text-slate-400">{c.threshold}</td>
                      <td className="py-2 text-slate-300">{c.details}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
              <h3 className="mb-4 text-sm font-semibold text-white">Null Rate (%) by Critical Column</h3>
              {nullRateData.length > 0 ? (
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={nullRateData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis dataKey="column" stroke="#94a3b8" fontSize={11} />
                      <YAxis stroke="#94a3b8" fontSize={11} />
                      <Tooltip
                        contentStyle={{ backgroundColor: "#0f172a", borderColor: "#334155" }}
                      />
                      <Bar dataKey="nullRatePct" name="Null %" fill="#38bdf8" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <p className="text-xs text-slate-400">No null-rate metrics recorded.</p>
              )}
            </div>

            <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
              <h3 className="mb-4 text-sm font-semibold text-white">Quarantine Failure Reasons</h3>
              {quarantineData.length > 0 ? (
                <div className="h-64">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={quarantineData} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                      <XAxis type="number" stroke="#94a3b8" fontSize={11} />
                      <YAxis type="category" dataKey="reason" width={150} stroke="#94a3b8" fontSize={10} />
                      <Tooltip
                        contentStyle={{ backgroundColor: "#0f172a", borderColor: "#334155" }}
                      />
                      <Bar dataKey="count" name="Rows" fill="#f59e0b" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <p className="text-xs text-slate-400">Zero quarantined rows in this batch.</p>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
