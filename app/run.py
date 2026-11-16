"""Start the collector web application (``python -m app.run``).

The application runs as two server processes (uvicorn workers).  Each process
serves the FastAPI app *and* runs a collection loop; the SQLite database and
the origin governor are shared, so the two collectors together behave like a
single polite client per source (see :mod:`app.governor`).
"""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.api:create_app",
        factory=True,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
        workers=int(os.environ.get("WEB_WORKERS", "2")),
    )
