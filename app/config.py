"""Runtime configuration shared by the API process and the worker processes."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _optional_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _optional_float(name: str) -> float | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw)
    except ValueError:
        return None


#: Requests to one origin must start at least one second apart.  The extra
#: 50 ms is a safety margin so scheduler/network jitter can never make two
#: observed request starts less than one second apart.
MIN_INTERVAL_SECONDS = 1.05


DEFAULT_DB = str(Path(__file__).resolve().parent.parent / "data" / "corpus.db")


@dataclass
class Settings:
    db_path: str = field(default_factory=lambda: os.environ.get("CORPUS_DB", DEFAULT_DB))
    worker_name: str = field(default_factory=lambda: os.environ.get("WORKER_NAME", "worker"))
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    enable_collector: bool = field(
        default_factory=lambda: _bool("ENABLE_COLLECTOR", True)
    )
    min_interval: float = field(
        default_factory=lambda: _float("MIN_REQUEST_INTERVAL", MIN_INTERVAL_SECONDS)
    )
    lease_seconds: float = field(
        default_factory=lambda: _float("ORIGIN_LEASE_SECONDS", 90.0)
    )
    #: ``None`` means derive from the lease (see ``worker.default_dead_after``).
    worker_dead_after: float | None = field(
        default_factory=lambda: _optional_float("WORKER_DEAD_AFTER")
    )
    retry_after_fallback: int = field(default_factory=lambda: _int("RETRY_AFTER_FALLBACK", 2))
    #: ``None`` means an integer Retry-After is honoured exactly as sent.
    max_retry_after: int | None = field(
        default_factory=lambda: _optional_int("MAX_RETRY_AFTER")
    )
    max_urls: int = field(default_factory=lambda: _int("MAX_URLS_PER_BATCH", 20))
    demo_a_url: str = field(
        default_factory=lambda: os.environ.get("DEMO_A_URL", "http://127.0.0.1:8001")
    )
    demo_b_url: str = field(
        default_factory=lambda: os.environ.get("DEMO_B_URL", "http://127.0.0.1:8002")
    )

    @property
    def static_dir(self) -> Path:
        return Path(__file__).resolve().parent / "static"

    def ensure_data_dir(self) -> None:
        parent = Path(self.db_path).resolve().parent
        parent.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    return Settings()
