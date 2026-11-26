import React from "react";

export function KpiCard({ title, value, subtitle, icon: Icon, accent = "text-sky-400" }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4 shadow-sm">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wider text-slate-400">
          {title}
        </span>
        {Icon && <Icon className={`h-4 w-4 ${accent}`} />}
      </div>
      <div className="mt-2 text-2xl font-bold text-slate-100">{value}</div>
      {subtitle && <div className="mt-1 text-xs text-slate-400">{subtitle}</div>}
    </div>
  );
}

export function LoadingPanel({ message = "Loading data..." }) {
  return (
    <div className="flex items-center justify-center rounded-xl border border-slate-800 bg-slate-900/50 p-10 text-sm text-slate-400">
      <div className="mr-3 h-4 w-4 animate-spin rounded-full border-2 border-sky-400 border-t-transparent" />
      {message}
    </div>
  );
}

export function ErrorBanner({ error, onRetry }) {
  if (!error) return null;
  return (
    <div className="flex items-center justify-between rounded-xl border border-rose-500/40 bg-rose-950/40 px-4 py-3 text-sm text-rose-200">
      <span>{error}</span>
      {onRetry && (
        <button
          onClick={onRetry}
          className="ml-4 rounded-lg border border-rose-400/40 px-3 py-1 text-xs font-semibold hover:bg-rose-900/50"
        >
          Retry
        </button>
      )}
    </div>
  );
}

export function EmptyState({ title, description, actionLabel, onAction }) {
  return (
    <div className="rounded-xl border border-dashed border-slate-700 bg-slate-900/40 p-10 text-center">
      <h3 className="text-base font-semibold text-slate-200">{title}</h3>
      <p className="mx-auto mt-1 max-w-md text-sm text-slate-400">{description}</p>
      {actionLabel && onAction && (
        <button
          onClick={onAction}
          className="mt-4 rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-500"
        >
          {actionLabel}
        </button>
      )}
    </div>
  );
}
