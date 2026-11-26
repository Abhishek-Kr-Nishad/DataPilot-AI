import React, { useEffect, useState } from "react";
import { Cloud, Layers, Play, Upload } from "lucide-react";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { ErrorBanner, LoadingPanel } from "../components/UiPrimitives";

export function PipelineManagementPage({ selectedRunId, onPipelineExecuted }) {
  const [scenario, setScenario] = useState("clean");
  const [numRows, setNumRows] = useState(250);
  const [seed, setSeed] = useState(42);
  const [failOnQuality, setFailOnQuality] = useState(true);
  const [csvFile, setCsvFile] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState("");
  const [lastResult, setLastResult] = useState(null);

  const [runs, setRuns] = useState([]);
  const [activeRun, setActiveRun] = useState(null);
  const [layerName, setLayerName] = useState("silver");
  const [layerPreview, setLayerPreview] = useState(null);
  const [awsStatus, setAwsStatus] = useState(null);
  const [athenaSql, setAthenaSql] = useState(null);
  const [loadingRuns, setLoadingRuns] = useState(true);

  const loadMetadata = async (targetId = selectedRunId) => {
    setLoadingRuns(true);
    try {
      const [runList, aws, sql] = await Promise.all([
        api.listRuns(20),
        api.getAwsStatus(),
        api.getAthenaSql()
      ]);
      setRuns(runList || []);
      setAwsStatus(aws);
      setAthenaSql(sql);

      const chosen =
        (targetId && runList.find((r) => r.run_id === targetId)) || runList[0] || null;
      setActiveRun(chosen);
      if (chosen && chosen.layers) {
        const available = Object.keys(chosen.layers);
        const preferred = available.includes(layerName) ? layerName : available[0];
        if (preferred) {
          setLayerName(preferred);
          const prev = await api.previewLayer(chosen.run_id, preferred, 25);
          setLayerPreview(prev);
        } else {
          setLayerPreview(null);
        }
      } else {
        setLayerPreview(null);
      }
    } catch (err) {
      setError(err.message || "Failed loading pipeline details.");
    } finally {
      setLoadingRuns(false);
    }
  };

  useEffect(() => {
    loadMetadata(selectedRunId);
  }, [selectedRunId]);

  const handleRunSynthetic = async (e) => {
    e.preventDefault();
    setRunning(true);
    setError("");
    try {
      const res = await api.runSyntheticPipeline({
        scenario,
        num_rows: Number(numRows),
        seed: Number(seed),
        fail_on_quality_error: failOnQuality
      });
      setLastResult(res);
      onPipelineExecuted?.(res.run_record.run_id);
      await loadMetadata(res.run_record.run_id);
    } catch (err) {
      setError(err.message || "Synthetic pipeline run failed.");
    } finally {
      setRunning(false);
    }
  };

  const handleUploadRun = async (e) => {
    e.preventDefault();
    if (!csvFile) {
      setError("Please select a .csv file first.");
      return;
    }
    setRunning(true);
    setError("");
    try {
      const res = await api.uploadCsvAndRun(csvFile, failOnQuality, false);
      setLastResult(res);
      onPipelineExecuted?.(res.run_record.run_id);
      await loadMetadata(res.run_record.run_id);
    } catch (err) {
      setError(err.message || "CSV upload run failed.");
    } finally {
      setRunning(false);
    }
  };

  const handlePreviewLayer = async (runId, nextLayer) => {
    setLayerName(nextLayer);
    setError("");
    try {
      const prev = await api.previewLayer(runId, nextLayer, 25);
      setLayerPreview(prev);
    } catch (err) {
      setError(err.message || "Unable to preview layer.");
    }
  };

  const handleAwsSync = async () => {
    if (!activeRun) return;
    setError("");
    try {
      await api.syncRunToAws(activeRun.run_id);
      await loadMetadata(activeRun.run_id);
    } catch (err) {
      setError(err.message || "AWS sync blocked.");
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-bold text-white">Pipeline Management</h2>
        <p className="text-sm text-slate-400">
          Execute PySpark Bronze → Silver → Quality → Gold jobs and explore materialized Parquet layers
        </p>
      </div>

      <ErrorBanner error={error} />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <form
          onSubmit={handleRunSynthetic}
          className="space-y-4 rounded-xl border border-slate-800 bg-slate-900/70 p-5"
        >
          <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
            <Play className="h-4 w-4 text-sky-400" />
            Synthetic E-Commerce Batch Runner
          </h3>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            <div>
              <label className="mb-1 block text-xs text-slate-400">Scenario</label>
              <select
                value={scenario}
                onChange={(e) => setScenario(e.target.value)}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
              >
                <option value="clean">clean</option>
                <option value="dirty_nulls_duplicates">dirty_nulls_duplicates</option>
                <option value="corrupt_prices">corrupt_prices</option>
                <option value="schema_drift">schema_drift</option>
                <option value="empty_file">empty_file</option>
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">Rows</label>
              <input
                type="number"
                min={1}
                max={10000}
                value={numRows}
                onChange={(e) => setNumRows(e.target.value)}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
              />
            </div>
            <div>
              <label className="mb-1 block text-xs text-slate-400">Seed</label>
              <input
                type="number"
                min={0}
                value={seed}
                onChange={(e) => setSeed(e.target.value)}
                className="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
              />
            </div>
          </div>

          <label className="flex items-center gap-2 text-xs text-slate-300">
            <input
              type="checkbox"
              checked={failOnQuality}
              onChange={(e) => setFailOnQuality(e.target.checked)}
              className="rounded border-slate-700 bg-slate-950"
            />
            Enforce Quality Gate (halt Gold aggregation when checks fail)
          </label>

          <button
            type="submit"
            disabled={running}
            className="w-full rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-500 disabled:opacity-50"
          >
            {running ? "Executing PySpark Job..." : "Trigger PySpark Pipeline"}
          </button>
        </form>

        <form
          onSubmit={handleUploadRun}
          className="space-y-4 rounded-xl border border-slate-800 bg-slate-900/70 p-5"
        >
          <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
            <Upload className="h-4 w-4 text-emerald-400" />
            Upload Raw CSV Dataset
          </h3>
          <p className="text-xs text-slate-400">
            Ingest a custom e-commerce orders CSV directly via multipart upload (`POST /api/v1/pipeline/upload`).
          </p>
          <input
            type="file"
            accept=".csv"
            onChange={(e) => setCsvFile(e.target.files?.[0] || null)}
            className="block w-full rounded-lg border border-slate-700 bg-slate-950 p-2 text-xs text-slate-300"
          />
          <button
            type="submit"
            disabled={running || !csvFile}
            className="w-full rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-500 disabled:opacity-50"
          >
            {running ? "Uploading & Processing..." : "Upload & Execute ETL"}
          </button>
        </form>
      </div>

      {lastResult && (
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-4 text-sm">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              Latest Triggered Run:{" "}
              <strong className="font-mono text-sky-300">{lastResult.run_record.run_id}</strong>
            </div>
            <StatusBadge value={lastResult.run_record.status} />
          </div>
          {lastResult.quality_report && (
            <p className="mt-2 text-xs text-slate-300">{lastResult.quality_report.summary}</p>
          )}
        </div>
      )}

      {loadingRuns ? (
        <LoadingPanel message="Loading Medallion layers..." />
      ) : activeRun && activeRun.layers ? (
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5 space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
              <Layers className="h-4 w-4 text-sky-400" />
              Medallion Layer Explorer ({activeRun.run_id})
            </h3>
            <div className="flex flex-wrap gap-2">
              {Object.keys(activeRun.layers).map((lk) => (
                <button
                  key={lk}
                  onClick={() => handlePreviewLayer(activeRun.run_id, lk)}
                  className={`rounded-lg px-3 py-1 text-xs font-medium ${
                    layerName === lk
                      ? "bg-sky-600 text-white"
                      : "border border-slate-700 bg-slate-950 text-slate-300 hover:bg-slate-800"
                  }`}
                >
                  {lk} ({activeRun.layers[lk].row_count})
                </button>
              ))}
            </div>
          </div>

          {layerPreview && layerPreview.records.length > 0 ? (
            <div className="max-h-96 overflow-auto rounded-lg border border-slate-800">
              <table className="w-full text-left text-xs">
                <thead className="sticky top-0 bg-slate-950 text-slate-400">
                  <tr>
                    {layerPreview.columns.map((col) => (
                      <th key={col} className="whitespace-nowrap px-3 py-2 font-semibold">
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70 bg-slate-900/40">
                  {layerPreview.records.map((row, idx) => (
                    <tr key={idx} className="hover:bg-slate-800/40">
                      {layerPreview.columns.map((col) => (
                        <td key={col} className="whitespace-nowrap px-3 py-1.5 text-slate-300">
                          {row[col] === null || row[col] === undefined ? "—" : String(row[col])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-xs text-slate-400">This layer has 0 records.</p>
          )}
        </div>
      ) : null}

      {awsStatus && (
        <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5 space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h3 className="flex items-center gap-2 text-sm font-semibold text-white">
              <Cloud className="h-4 w-4 text-orange-400" />
              AWS S3, Glue & Athena Readiness
            </h3>
            <button
              onClick={handleAwsSync}
              disabled={!activeRun || !awsStatus.aws?.ready_for_aws}
              className="rounded-lg border border-orange-500/40 px-3 py-1.5 text-xs font-semibold text-orange-300 hover:bg-orange-950/40 disabled:opacity-40"
            >
              Sync Run to S3
            </button>
          </div>
          <p className="text-xs text-slate-400">
            Bucket: <code className="text-slate-200">{awsStatus.aws?.s3_bucket}</code> · Glue Job:{" "}
            <code className="text-slate-200">{awsStatus.aws?.glue_job_name}</code> · Athena DB:{" "}
            <code className="text-slate-200">{athenaSql?.database}</code>
          </p>
        </div>
      )}
    </div>
  );
}
