"""FastAPI application: submit batches, inspect pages, cancel, download JSONL."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import __version__
from .config import Settings, get_settings
from .db import Repository
from .fetch import normalize_url
from .governor import OriginGovernor
from .worker import Worker, default_dead_after

log = logging.getLogger("collector.api")


class BatchRequest(BaseModel):
    urls: list[str] = Field(default_factory=list)
    # Convenience: one URL per line, as typed into the browser textarea.
    text: str | None = None

    def url_list(self) -> list[str]:
        items: list[str] = []
        if self.urls:
            items.extend(self.urls)
        if self.text:
            for chunk in self.text.replace("\r\n", "\n").split("\n"):
                if chunk.strip():
                    items.append(chunk.strip())
        return items


def _source_status(governor: OriginGovernor, origin: str) -> dict[str, Any]:
    """Human-readable governor state for one origin of a batch."""
    state = governor.state(origin)
    now = time.time()
    return {
        "origin": origin,
        "cooldown_remaining": max(0.0, state.cooldown_until - now),
        "next_request_allowed_in": max(0.0, state.next_allowed_at - now),
    }


def build_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    settings.ensure_data_dir()
    repo = Repository(settings.db_path)
    governor = OriginGovernor(
        settings.db_path,
        owner=f"{settings.worker_name}-api",
        min_interval=settings.min_interval,
        lease_seconds=settings.lease_seconds,
    )
    dead_after = (
        settings.worker_dead_after
        if settings.worker_dead_after is not None
        else default_dead_after(settings.lease_seconds)
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker: Worker | None = None
        task: asyncio.Task[None] | None = None
        if settings.enable_collector:
            worker = Worker(
                repo,
                OriginGovernor(
                    settings.db_path,
                    owner=settings.worker_name,
                    min_interval=settings.min_interval,
                    lease_seconds=settings.lease_seconds,
                ),
                name=settings.worker_name,
                retry_after_fallback=settings.retry_after_fallback,
                max_retry_after=settings.max_retry_after,
                dead_after=dead_after,
            )
            task = asyncio.create_task(worker.run(), name=f"collector:{settings.worker_name}")
            log.info("collector loop started in API process (%s)", settings.worker_name)
        try:
            yield
        finally:
            if worker and task:
                worker.request_stop()
                try:
                    await asyncio.wait_for(task, timeout=10)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    task.cancel()

    app = FastAPI(title="文章语料采集器", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.repo = repo
    app.state.governor = governor
    app.state.worker_dead_after = dead_after

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "version": __version__}

    @app.get("/api/config")
    async def config() -> dict[str, Any]:
        return {
            "max_urls": settings.max_urls,
            "min_request_interval": settings.min_interval,
            "lease_seconds": settings.lease_seconds,
            "worker_dead_after": dead_after,
            # Sample URLs for the browser must be reachable by the collector;
            # DEMO_*_URL is set to the compose service names inside Docker.
            "demo_sources": [
                {"name": "演示源 A", "base": settings.demo_a_url},
                {"name": "演示源 B", "base": settings.demo_b_url},
            ],
        }

    @app.get("/api/workers")
    async def workers() -> dict[str, Any]:
        """Liveness of every collector process (heartbeat-based)."""
        return {
            "workers": repo.live_workers(dead_after),
            "dead_after": dead_after,
            "now": time.time(),
        }

    @app.post("/api/batches", status_code=201)
    async def create_batch(payload: BatchRequest, request: Request) -> JSONResponse:
        raw_urls = payload.url_list()
        if not raw_urls:
            raise HTTPException(status_code=400, detail="请至少提交 1 个文章 URL")
        if len(raw_urls) > settings.max_urls:
            raise HTTPException(
                status_code=400,
                detail=f"单批最多 {settings.max_urls} 个 URL，本次提交了 {len(raw_urls)} 个",
            )
        parsed: list[tuple[str, str]] = []
        seen: set[str] = set()
        errors: list[str] = []
        for raw in raw_urls:
            if not isinstance(raw, str) or not raw.strip():
                errors.append("空的 URL")
                continue
            try:
                url, origin = normalize_url(raw)
            except ValueError as exc:
                errors.append(f"{raw.strip() or '(空)'}：{exc}")
                continue
            if url in seen:
                continue
            seen.add(url)
            parsed.append((url, origin))
        if errors:
            raise HTTPException(
                status_code=400,
                detail={"message": "存在无效 URL", "errors": errors},
            )
        if not parsed:
            raise HTTPException(status_code=400, detail="没有可采集的 URL")
        batch_id = repo.create_batch(parsed)
        repo.record_event(batch_id, "created", f"批次已创建，共 {len(parsed)} 页")
        location = request.url_for("get_batch", batch_id=batch_id)
        return JSONResponse(
            status_code=201,
            content={"batch_id": batch_id, "total": len(parsed)},
            headers={"Location": str(location)},
        )

    @app.get("/api/batches")
    async def list_batches() -> dict[str, Any]:
        return {"batches": repo.list_batches()}

    @app.get("/api/batches/{batch_id}")
    async def get_batch(batch_id: str) -> dict[str, Any]:
        batch = repo.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        batch["sources"] = [
            _source_status(governor, origin)
            for origin in sorted({p["origin"] for p in batch["pages"]})
        ]
        return batch

    @app.post("/api/batches/{batch_id}/cancel")
    async def cancel_batch(batch_id: str) -> dict[str, Any]:
        batch = repo.cancel_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        return batch

    @app.get("/api/batches/{batch_id}/corpus.jsonl")
    async def download_corpus(batch_id: str) -> StreamingResponse:
        batch = repo.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")

        def lines():
            for row in repo.corpus_rows(batch_id):
                record = {"title": row["title"], "body": row["body"], "url": row["url"]}
                yield json.dumps(record, ensure_ascii=False) + "\n"

        headers = {
            "Content-Disposition": f'attachment; filename="corpus-{batch_id}.jsonl"',
            "Cache-Control": "no-store",
        }
        return StreamingResponse(
            lines(),
            media_type="application/x-ndjson; charset=utf-8",
            headers=headers,
        )

    @app.get("/api/batches/{batch_id}/corpus")
    async def corpus_preview(batch_id: str) -> dict[str, Any]:
        batch = repo.get_batch(batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        rows = repo.corpus_rows(batch_id)
        return {
            "batch_id": batch_id,
            "count": len(rows),
            "records": [
                {"title": r["title"], "body": r["body"], "url": r["url"]} for r in rows
            ],
        }

    @app.get("/api/demo/{source}/requests")
    async def demo_requests(source: str) -> dict[str, Any]:
        """Proxy a demo source's request log so the browser can inspect it."""
        bases = {"a": settings.demo_a_url, "b": settings.demo_b_url}
        base = bases.get(source)
        if not base:
            raise HTTPException(status_code=404, detail="未知演示源")
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{base.rstrip('/')}/requests.json")
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"无法读取演示源请求日志：{exc.__class__.__name__}"
            ) from exc

    static_dir = settings.static_dir

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    return app


def create_app() -> FastAPI:
    """Factory for ``uvicorn app.api:create_app --factory``."""
    return build_app(get_settings())
