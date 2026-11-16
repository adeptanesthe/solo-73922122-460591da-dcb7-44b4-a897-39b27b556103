"""Local demonstration news source.

Two instances run in docker-compose (``demo-a`` and ``demo-b``, exposed on
host ports 8001/8002).  An article URL may carry tuning parameters:

* ``delay=<seconds>``  – sleep before answering (simulates a slow source);
* ``fail=<status>``    – always answer with this HTTP status;
* ``limit=<n>``        – answer 429 with ``Retry-After: <retry_after>`` for
  the first ``n`` requests to that exact URL, then serve the article;
* ``retry_after=<s>``  – integer seconds sent in the 429 ``Retry-After``
  header (default 2).

Every received request is appended to an in-memory request log together with
its receive and completion timestamps; the log is served as JSON from
``/requests.json`` (and proxied by the collector web app under
``/api/demo/a/requests`` and ``/api/demo/b/requests``).
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections import defaultdict, deque
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

SLUGS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
TITLES = {
    "alpha": "首条演示新闻：城市更新",
    "beta": "第二条演示新闻：港口贸易",
    "gamma": "第三条演示新闻：科研成果",
    "delta": "第四条演示新闻：社区生活",
    "epsilon": "第五条演示新闻：天气变化",
    "zeta": "第六条演示新闻：体育赛事",
}


class RequestLog:
    """Thread-safe, bounded log of requests served by one demo source."""

    def __init__(self, capacity: int = 5000) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict[str, Any]] = deque(maxlen=capacity)

    def start(self, method: str, path: str, query: str) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "method": method,
            "path": path,
            "query": query,
            "received_at": time.time(),
            "finished_at": None,
            "status": None,
        }
        with self._lock:
            self._entries.append(entry)
        return entry

    def finish(self, entry: dict[str, Any], status: int) -> None:
        with self._lock:
            entry["finished_at"] = time.time()
            entry["status"] = status

    def all(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self._entries]

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


class PageCounter:
    """Per-URL request counter (used for one-shot 429 / fail scenarios)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = defaultdict(int)

    def hit(self, key: str) -> int:
        with self._lock:
            self._counts[key] += 1
            return self._counts[key]

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


def _int(value: str | None, default: int, low: int, high: int) -> int:
    try:
        number = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def _float(value: str | None, default: float, high: float = 60.0) -> float:
    try:
        number = float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return max(0.0, min(high, number))


def _article_html(name: str, slug: str, title: str) -> str:
    paragraphs = [
        f"这是演示源 {name} 的《{title}》。",
        f"Slug 为 {slug}，用于验证标题、正文与来源 URL 的采集结果。",
        "采集器只请求给定的 URL，不跟随任何重定向。",
        "段落之间用空行分隔，便于人工检查 JSONL 内容。",
        "本文来自本地演示源，仅用于测试采集器。",
    ]
    body = "\n".join(f"<p>{text}</p>" for text in paragraphs)
    return (
        '<!doctype html><html lang="zh"><head><meta charset="utf-8">'
        f"<title>{title} · {name}</title>"
        f'<meta property="og:title" content="{title}">'
        "</head><body>"
        '<header><nav><a href="/">首页</a></nav></header>'
        "<main>"
        f'<article data-source="{name}" data-slug="{slug}">'
        f"<h1>{title}</h1>"
        '<p class="byline">记者 / 演示源编辑部</p>'
        f"{body}"
        "</article>"
        "</main>"
        "<footer>演示站点</footer></body></html>"
    )


def create_app(name: str = "demo") -> FastAPI:
    app = FastAPI(title=f"演示新闻源 {name}")
    log = RequestLog()
    counter = PageCounter()

    app.state.name = name
    app.state.log = log
    app.state.counter = counter

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        items = "".join(
            f'<li><a href="/article/{slug}">{slug}</a></li>' for slug in SLUGS
        )
        return (
            f"<html><body><h1>演示新闻源 {name}</h1><ul>{items}</ul>"
            "<p>参数：<code>?delay=1.5&amp;fail=500&amp;limit=2&amp;retry_after=3</code></p>"
            '<p><a href="/requests.json">请求日志</a></p></body></html>'
        )

    @app.get("/article/{slug}", response_class=HTMLResponse)
    async def article(slug: str, request: Request) -> Response:
        delay = _float(request.query_params.get("delay"), 0.0)
        fail = _int(request.query_params.get("fail"), 0, 0, 599)
        limit = _int(request.query_params.get("limit"), 0, 0, 1000)
        retry_after = _int(request.query_params.get("retry_after"), 2, 0, 600)

        entry = log.start(request.method, request.url.path, request.url.query)
        key = f"{slug}|{request.url.query}"

        if fail:
            status = fail
            headers = {"Cache-Control": "no-store"}
            body = f"<html><body><h1>{fail}</h1><p>模拟失败</p></body></html>"
        elif limit and counter.hit(key) <= limit:
            status = 429
            headers = {"Cache-Control": "no-store", "Retry-After": str(retry_after)}
            body = "<html><body><h1>429</h1><p>Too Many Requests</p></body></html>"
        else:
            title = TITLES.get(slug, f"演示文章 {slug}")
            status = 200
            headers = {"Cache-Control": "no-store"}
            body = _article_html(name, slug, title)

        if delay:
            await asyncio.sleep(delay)
        log.finish(entry, status)
        return Response(
            content=body,
            status_code=status,
            headers=headers,
            media_type="text/html; charset=utf-8",
        )

    @app.get("/redirect/{slug}")
    async def redirecting(slug: str, request: Request) -> Response:
        entry = log.start(request.method, request.url.path, request.url.query)
        log.finish(entry, 302)
        return Response(
            status_code=302,
            headers={"Location": f"/article/{slug}", "Cache-Control": "no-store"},
            media_type="text/plain",
        )

    @app.get("/no-article/{slug}", response_class=HTMLResponse)
    async def no_article(slug: str, request: Request) -> Response:
        entry = log.start(request.method, request.url.path, request.url.query)
        log.finish(entry, 200)
        return HTMLResponse(
            "<html><body><h1>没有正文</h1><div>这个页面没有 article 元素。</div></body></html>"
        )

    @app.get("/requests")
    @app.get("/requests.json")
    async def requests_log() -> JSONResponse:
        return JSONResponse({"source": name, "requests": log.all()})

    @app.post("/reset")
    async def reset() -> JSONResponse:
        log.clear()
        counter.reset()
        return JSONResponse({"ok": True})

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "source": name}

    return app


app = create_app(os.environ.get("DEMO_NAME", "demo"))
