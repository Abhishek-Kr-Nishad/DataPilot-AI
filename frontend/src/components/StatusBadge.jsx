import React from "react";

const STYLES = {
  SUCCESS: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
  QUALITY_FAILED: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  FAILED: "bg-rose-500/15 text-rose-300 border-rose-500/30",
  RUNNING: "bg-sky-500/15 text-sky-300 border-sky-500/30",
  PENDING_APPROVAL: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  APPROVED: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
  REJECTED: "bg-rose-500/15 text-rose-300 border-rose-500/30",
  LOCAL: "bg-indigo-500/15 text-indigo-300 border-indigo-500/30",
  AWS: "bg-orange-500/15 text-orange-300 border-orange-500/30"
};

export function StatusBadge({ value }) {
  const key = String(value || "UNKNOWN").toUpperCase();
  const cls = STYLES[key] || "bg-slate-700/40 text-slate-300 border-slate-600";
  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold tracking-wide ${cls}`}
    >
      {key}
    </span>
  );
}
