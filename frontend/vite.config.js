import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const BACKEND_URL = "http://127.0.0.1:8000";

function attachProxyErrorHandler(proxy) {
  proxy.on("error", (err, _req, res) => {
    if (res && !res.headersSent) {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          detail: `FastAPI backend (${BACKEND_URL}) is unreachable (${err.code || err.message}). Start backend via scripts/run_api.py.`
        })
      );
    }
  });
}

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      "/health": {
        target: BACKEND_URL,
        changeOrigin: true,
        configure: attachProxyErrorHandler
      },
      "/api": {
        target: BACKEND_URL,
        changeOrigin: true,
        configure: attachProxyErrorHandler
      },
      "/docs": {
        target: BACKEND_URL,
        changeOrigin: true,
        configure: attachProxyErrorHandler
      },
      "/openapi.json": {
        target: BACKEND_URL,
        changeOrigin: true,
        configure: attachProxyErrorHandler
      }
    }
  }
});
