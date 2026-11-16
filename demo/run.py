"""Serve one demonstration source: ``uvicorn demo.source:app``."""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "demo.source:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8001")),
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
        access_log=True,
    )
