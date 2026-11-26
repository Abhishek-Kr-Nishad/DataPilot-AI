"""CLI entrypoint to start the DataPilot AI FastAPI server with Uvicorn."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the DataPilot AI FastAPI server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    uvicorn.run("datapilot.api.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
