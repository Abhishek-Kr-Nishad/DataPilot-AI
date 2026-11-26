import React, { useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { DollarSign, ShoppingBag, TrendingUp, Users } from "lucide-react";
import { api } from "../api/client";
import { EmptyState, ErrorBanner, KpiCard, LoadingPanel } from "../components/UiPrimitives";

export function AnalyticsPage({ selectedRunId, refreshToken }) {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await api.getAnalyticsSummary(selectedRunId);
      setSummary(data);
    } catch (err) {
      setError(err.message || "Failed to load Gold analytics summary.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, [selectedRunId, refreshToken]);

  if (loading) return <LoadingPanel message="Loading Gold layer analytics..." />;

  if (!summary?.has_data) {
    return (
      <EmptyState
        title="No Gold Analytics Available"
        description="Execute a clean pipeline run that passes the data quality gate to materialize Gold daily revenue, category performance, regional sales, and customer LTV tables."
      />
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-white">Gold Layer Analytics</h2>
        <p className="text-sm text-slate-400">
          E-commerce revenue, category, regional, and customer LTV insights from{" "}
          <span className="font-mono text-sky-300">{summary.run_id}</span>
        </p>
      </div>

      <ErrorBanner error={error} onRetry={load} />

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          title="Gross Revenue"
          value={`$${summary.total_gross_revenue.toLocaleString()}`}
          icon={DollarSign}
          accent="text-sky-400"
        />
        <KpiCard
          title="Net Revenue"
          value={`$${summary.total_net_revenue.toLocaleString()}`}
          icon={TrendingUp}
          accent="text-emerald-400"
        />
        <KpiCard
          title="Total Discounts"
          value={`$${summary.total_discounts.toLocaleString()}`}
          icon={ShoppingBag}
          accent="text-amber-400"
        />
        <KpiCard
          title="Average Order Value"
          value={`$${summary.avg_order_value.toFixed(2)}`}
          icon={Users}
          accent="text-indigo-400"
        />
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <h3 className="mb-4 text-sm font-semibold text-white">Daily Revenue Trend</h3>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={summary.daily_revenue}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="order_date" stroke="#94a3b8" fontSize={11} />
                <YAxis stroke="#94a3b8" fontSize={11} />
                <Tooltip contentStyle={{ backgroundColor: "#0f172a", borderColor: "#334155" }} />
                <Area
                  type="monotone"
                  dataKey="net_revenue"
                  name="Net Revenue"
                  stroke="#10b981"
                  fill="#10b981"
                  fillOpacity={0.2}
                />
                <Area
                  type="monotone"
                  dataKey="gross_revenue"
                  name="Gross Revenue"
                  stroke="#38bdf8"
                  fill="#38bdf8"
                  fillOpacity={0.12}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <h3 className="mb-4 text-sm font-semibold text-white">Net Revenue by Category</h3>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={summary.category_performance}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="category" stroke="#94a3b8" fontSize={11} />
                <YAxis stroke="#94a3b8" fontSize={11} />
                <Tooltip contentStyle={{ backgroundColor: "#0f172a", borderColor: "#334155" }} />
                <Bar dataKey="net_revenue" name="Net Revenue" fill="#38bdf8" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <h3 className="mb-3 text-sm font-semibold text-white">Regional Sales Breakdown</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-800 text-slate-400">
                <tr>
                  <th className="py-2 pr-3">Region</th>
                  <th className="py-2 pr-3">Country</th>
                  <th className="py-2 pr-3">Orders</th>
                  <th className="py-2">Net Revenue</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/70">
                {summary.regional_sales.map((r, i) => (
                  <tr key={i}>
                    <td className="py-2 pr-3 text-slate-200">{r.region}</td>
                    <td className="py-2 pr-3 text-slate-400">{r.country} ({r.currency})</td>
                    <td className="py-2 pr-3 text-slate-300">{r.total_orders}</td>
                    <td className="py-2 font-semibold text-emerald-300">${r.net_revenue}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
          <h3 className="mb-3 text-sm font-semibold text-white">Top Customers (LTV Summary)</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-800 text-slate-400">
                <tr>
                  <th className="py-2 pr-3">Customer ID</th>
                  <th className="py-2 pr-3">Orders</th>
                  <th className="py-2 pr-3">Units</th>
                  <th className="py-2">Lifetime Spend</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/70">
                {summary.top_customers.slice(0, 10).map((c) => (
                  <tr key={c.customer_id}>
                    <td className="py-2 pr-3 font-mono text-sky-300">{c.customer_id}</td>
                    <td className="py-2 pr-3 text-slate-300">{c.order_count}</td>
                    <td className="py-2 pr-3 text-slate-300">{c.total_units}</td>
                    <td className="py-2 font-semibold text-emerald-300">${c.lifetime_net_spend}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
