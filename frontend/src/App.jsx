import React, { useEffect, useState } from "react";
import {
  Activity,
  BarChart3,
  Bot,
  Database,
  History,
  RefreshCw,
  ShieldCheck,
  Workflow
} from "lucide-react";
import { api } from "./api/client";
import { StatusBadge } from "./components/StatusBadge";
import { AgentAssistantPage } from "./pages/AgentAssistantPage";
import { AnalyticsPage } from "./pages/AnalyticsPage";
import { DataQualityPage } from "./pages/DataQualityPage";
import { HistoryPage } from "./pages/HistoryPage";
import { OverviewPage } from "./pages/OverviewPage";
import { PipelineManagementPage } from "./pages/PipelineManagementPage";

const NAV_ITEMS = [
  { id: "overview", label: "Overview Dashboard", icon: Activity },
  { id: "pipelines", label: "Pipeline Management", icon: Workflow },
  { id: "quality", label: "Data Quality Monitoring", icon: ShieldCheck },
  { id: "agent", label: "AI Agent Assistant", icon: Bot },
  { id: "analytics", label: "Analytics", icon: BarChart3 },
  { id: "history", label: "Execution History", icon: History }
];

export default function App() {
  const [activePage, setActivePage] = useState("overview");
  const [selectedRunId, setSelectedRunId] = useState("");
  const [health, setHealth] = useState(null);
  const [runs, setRuns] = useState([]);
  const [headerError, setHeaderError] = useState("");
  const [refreshToken, setRefreshToken] = useState(0);

  const loadHeader = async () => {
    setHeaderError("");
    try {
      const [h, rList] = await Promise.all([api.getHealth(), api.listRuns(30)]);
      setHealth(h);
      setRuns(rList || []);
      if (!selectedRunId && rList && rList.length > 0) {
        setSelectedRunId(rList[0].run_id);
      }
    } catch (err) {
      setHeaderError(err?.message || "Backend API unreachable");
    }
  };

  useEffect(() => {
    loadHeader();
  }, [refreshToken]);

  const handlePipelineExecuted = (newRunId) => {
    if (newRunId) setSelectedRunId(newRunId);
    setRefreshToken((n) => n + 1);
  };

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-100">
      <aside className="flex w-64 shrink-0 flex-col border-r border-slate-800 bg-slate-900/80 p-4">
        <div className="flex items-center gap-2.5 px-2 pb-5">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-sky-600/20 text-sky-400">
            <Database className="h-5 w-5" />
          </div>
          <div>
            <h1 className="text-sm font-bold tracking-tight text-white">DataPilot AI</h1>
            <p className="text-[11px] text-slate-400">Agentic AWS Lakehouse</p>
          </div>
        </div>

        <nav className="flex-1 space-y-1">
          {NAV_ITEMS.map(({ id, label, icon: Icon }) => {
            const active = activePage === id;
            return (
              <button
                key={id}
                onClick={() => setActivePage(id)}
                className={`flex w-full items-center gap-2.5 rounded-lg px-3 py-2 text-left text-xs font-medium transition ${
                  active
                    ? "bg-sky-600/20 text-sky-300 border border-sky-500/30"
                    : "text-slate-300 hover:bg-slate-800/70"
                }`}
              >
                <Icon className="h-4 w-4" />
                {label}
              </button>
            );
          })}
        </nav>

        <div className="mt-4 space-y-2 border-t border-slate-800 pt-4 text-xs">
          <label className="block text-[11px] font-medium text-slate-400">Active Run Context</label>
          <select
            value={selectedRunId}
            onChange={(e) => setSelectedRunId(e.target.value)}
            className="w-full rounded-lg border border-slate-700 bg-slate-950 px-2.5 py-1.5 text-xs text-slate-200"
          >
            <option value="">Latest Run</option>
            {runs.map((r) => (
              <option key={r.run_id} value={r.run_id}>
                {r.run_id} ({r.status})
              </option>
            ))}
          </select>

          <button
            onClick={() => setRefreshToken((n) => n + 1)}
            className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-slate-700 px-3 py-1.5 text-xs text-slate-300 hover:bg-slate-800"
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh API Data
          </button>

          {headerError && (
            <div className="rounded-lg border border-rose-500/40 bg-rose-950/40 p-2 text-[11px] text-rose-200">
              API Connection: {headerError}
            </div>
          )}

          {health && (
            <div className="flex items-center justify-between pt-1 text-[11px] text-slate-400">
              <span>Mode:</span>
              <StatusBadge value={health.effective_mode} />
            </div>
          )}
        </div>
      </aside>

      <main className="flex-1 overflow-y-auto p-6 lg:p-8">
        {activePage === "overview" && (
          <OverviewPage
            health={health}
            selectedRunId={selectedRunId}
            onSelectRun={setSelectedRunId}
            onNavigate={setActivePage}
            refreshToken={refreshToken}
            onPipelineExecuted={handlePipelineExecuted}
          />
        )}
        {activePage === "pipelines" && (
          <PipelineManagementPage
            selectedRunId={selectedRunId}
            onPipelineExecuted={handlePipelineExecuted}
          />
        )}
        {activePage === "quality" && (
          <DataQualityPage selectedRunId={selectedRunId} refreshToken={refreshToken} />
        )}
        {activePage === "agent" && (
          <AgentAssistantPage selectedRunId={selectedRunId} refreshToken={refreshToken} />
        )}
        {activePage === "analytics" && (
          <AnalyticsPage selectedRunId={selectedRunId} refreshToken={refreshToken} />
        )}
        {activePage === "history" && (
          <HistoryPage
            selectedRunId={selectedRunId}
            onSelectRun={setSelectedRunId}
            onNavigate={setActivePage}
            refreshToken={refreshToken}
          />
        )}
      </main>
    </div>
  );
}
