const API_BASE = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const ENV_API_KEY = (import.meta.env.VITE_API_KEY || "").trim();

function getApiKey() {
  if (ENV_API_KEY) return ENV_API_KEY;
  try {
    return (window?.localStorage?.getItem("datapilot_api_key") || "").trim();
  } catch {
    return "";
  }
}

async function request(path, options = {}) {
  const apiKey = getApiKey();
  let res;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      ...options,
      headers: {
        ...(options.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
        ...(apiKey ? { "X-API-Key": apiKey } : {}),
        ...(options.headers || {})
      }
    });
  } catch (err) {
    throw new Error(
      `Network error reaching FastAPI for ${path}: ${err?.message || "connection failed"}`
    );
  }
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!res.ok) {
    let detail;
    if (data && typeof data === "object" && data.detail) {
      detail =
        typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail);
    } else if (typeof data === "string" && data.trim()) {
      detail = `HTTP ${res.status}: ${data.trim().slice(0, 220)}`;
    } else {
      detail = `HTTP ${res.status} on ${path}`;
    }
    throw new Error(detail);
  }
  return data;
}

export const api = {
  getHealth: () => request("/health"),
  getPipelineStatus: (runId) =>
    request(`/api/v1/pipeline/status${runId ? `?run_id=${encodeURIComponent(runId)}` : ""}`),
  listRuns: (limit = 50, status) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status && status !== "ALL") params.set("status", status);
    return request(`/api/v1/pipeline/runs?${params.toString()}`);
  },
  runSyntheticPipeline: (payload) =>
    request("/api/v1/pipeline/run", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  uploadCsvAndRun: (file, failOnQualityError = true, syncToAws = false) => {
    const form = new FormData();
    form.append("file", file);
    form.append("fail_on_quality_error", String(failOnQualityError));
    form.append("sync_to_aws", String(syncToAws));
    return request("/api/v1/pipeline/upload", {
      method: "POST",
      body: form
    });
  },
  previewLayer: (runId, layer, limit = 100) =>
    request(
      `/api/v1/pipeline/runs/${encodeURIComponent(runId)}/layers/${encodeURIComponent(
        layer
      )}?limit=${limit}`
    ),
  getQualityReport: (runId) =>
    request(`/api/v1/quality/report${runId ? `?run_id=${encodeURIComponent(runId)}` : ""}`),
  getAnalyticsSummary: (runId) =>
    request(`/api/v1/analytics/summary${runId ? `?run_id=${encodeURIComponent(runId)}` : ""}`),
  diagnoseRun: (runId, mode = "auto") =>
    request("/api/v1/agent/diagnose", {
      method: "POST",
      body: JSON.stringify({ run_id: runId || null, mode })
    }),
  recommendRecovery: (runId, mode = "auto") =>
    request("/api/v1/agent/recommend", {
      method: "POST",
      body: JSON.stringify({ run_id: runId || null, mode })
    }),
  listRecommendations: (runId) =>
    request(
      `/api/v1/agent/recommendations${runId ? `?run_id=${encodeURIComponent(runId)}` : ""}`
    ),
  approveRecommendation: (recId, reviewer, notes) =>
    request(`/api/v1/agent/approvals/${encodeURIComponent(recId)}/approve`, {
      method: "POST",
      body: JSON.stringify({ reviewer, notes })
    }),
  rejectRecommendation: (recId, reviewer, notes) =>
    request(`/api/v1/agent/approvals/${encodeURIComponent(recId)}/reject`, {
      method: "POST",
      body: JSON.stringify({ reviewer, notes })
    }),
  listApprovalAudit: () => request("/api/v1/agent/approvals/audit"),
  getAwsStatus: () => request("/api/v1/aws/status"),
  getAthenaSql: () => request("/api/v1/aws/athena-sql"),
  syncRunToAws: (runId) =>
    request(`/api/v1/aws/sync/${encodeURIComponent(runId)}`, { method: "POST" })
};
