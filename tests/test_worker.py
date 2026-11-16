"""Storage/worker tests with a local stub source (no Docker required).

These tests exercise the real worker against ``demo.source`` served by a
uvicorn instance on an ephemeral port, using a temporary SQLite database.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest
import uvicorn

from app.db import Repository
from app.governor import OriginGovernor
from app.worker import Worker


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class DemoServer:
    def __init__(self) -> None:
        import demo.source as source

        self.app = source.create_app("demo-test")
        self.port = _free_port()
        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> "DemoServer":
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)
        assert self.server.started, "demo server did not start"
        return self

    def __exit__(self, *exc) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def requests(self) -> list[dict]:
        return self.app.state.log.all()

    def reset(self) -> None:
        self.app.state.log.clear()
        self.app.state.counter.reset()


@pytest.fixture()
def demo():
    with DemoServer() as server:
        yield server


def make_worker(tmp_path, name="w1", **kwargs) -> tuple[Worker, Repository]:
    db = str(tmp_path / "corpus.db")
    repo = Repository(db)
    governor = OriginGovernor(db, owner=name, min_interval=1.0)
    return (
        Worker(
            repo,
            governor,
            name=name,
            retry_after_fallback=kwargs.pop("retry_after_fallback", 2),
            **kwargs,
        ),
        repo,
    )


async def run_until(repo: Repository, batch_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        batch = repo.get_batch(batch_id)
        assert batch is not None
        pending = [p for p in batch["pages"] if p["status"] in ("queued", "backoff", "running")]
        if not pending:
            return batch
        await asyncio.sleep(0.1)
    raise AssertionError(f"batch {batch_id} did not finish: {repo.get_batch(batch_id)}")


async def collect(demo, tmp_path, urls: list[str], *, worker_kwargs=None):
    worker, repo = make_worker(tmp_path, **(worker_kwargs or {}))
    batch_id = repo.create_batch([(url, _origin(url)) for url in urls])
    task = asyncio.create_task(worker.run())
    try:
        batch = await run_until(repo, batch_id)
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, timeout=5)
    return batch, repo, worker


def _origin(url: str) -> str:
    from app.fetch import normalize_url

    return normalize_url(url)[1]


def test_collects_title_body_and_url(demo, tmp_path):
    urls = [demo.url("/article/alpha")]
    batch, repo, _ = asyncio.run(collect(demo, tmp_path, urls))
    pages = batch["pages"]
    assert pages[0]["status"] == "ok", pages
    assert pages[0]["title"] == "首条演示新闻：城市更新"
    rows = repo.corpus_rows(batch["id"])
    assert len(rows) == 1
    assert rows[0]["url"] == urls[0]
    assert "这是演示源 demo-test 的《首条演示新闻：城市更新》" in rows[0]["body"]
    assert rows[0]["title"] == "首条演示新闻：城市更新"


def test_requests_to_one_origin_are_spaced_and_never_overlap(demo, tmp_path):
    urls = [demo.url(f"/article/{slug}") for slug in ("alpha", "beta", "gamma")]
    batch, _, _ = asyncio.run(collect(demo, tmp_path, urls))
    assert all(p["status"] == "ok" for p in batch["pages"])
    entries = [e for e in demo.requests() if e["path"].startswith("/article/")]
    assert len(entries) == 3
    starts = sorted(e["received_at"] for e in entries)
    for earlier, later in zip(starts, starts[1:]):
        assert later - earlier >= 0.99, (starts, "请求开始间隔不足 1 秒")
    spans = sorted((e["received_at"], e["finished_at"]) for e in entries)
    for (_, first_end), (second_start, _) in zip(spans, spans[1:]):
        assert second_start >= first_end - 0.001, "同源请求发生重叠"


def test_redirect_is_failure_and_body_not_stored(demo, tmp_path):
    url = demo.url("/redirect/alpha")
    batch, repo, _ = asyncio.run(collect(demo, tmp_path, [url]))
    page = batch["pages"][0]
    assert page["status"] == "failed"
    assert page["error_code"] == "redirect"
    assert page["http_status"] == 302
    assert repo.corpus_rows(batch["id"]) == []
    # Only the given URL was requested; the redirect target was not followed.
    paths = [e["path"] for e in demo.requests()]
    assert paths == ["/redirect/alpha"]


def test_page_without_article_is_failure(demo, tmp_path):
    batch, repo, _ = asyncio.run(collect(demo, tmp_path, [demo.url("/no-article/alpha")]))
    page = batch["pages"][0]
    assert page["status"] == "failed"
    assert page["error_code"] == "no_article"
    assert repo.corpus_rows(batch["id"]) == []


def test_429_with_retry_after_pauses_and_retries_once(demo, tmp_path):
    url = demo.url("/article/alpha?limit=1&retry_after=2")
    batch, repo, _ = asyncio.run(collect(demo, tmp_path, [url]))
    page = batch["pages"][0]
    assert page["status"] == "ok", page
    assert page["retry_count"] == 1
    entries = demo.requests()
    assert [e["status"] for e in entries] == [429, 200]
    gap = entries[1]["received_at"] - entries[0]["received_at"]
    assert gap >= 2.0, f"未等待 Retry-After（实际 {gap:.2f}s）"
    assert "limit=1" in entries[1]["query"] or entries[1]["query"] == "limit=1&retry_after=2"
    assert repo.corpus_rows(batch["id"])


def test_429_cooldown_blocks_other_batch_same_origin(demo, tmp_path):
    """A 429 in one batch pauses every batch on that origin."""
    worker, repo = make_worker(tmp_path, name="w1")
    origin = _origin(demo.url("/"))
    other = demo.url("/article/beta")
    trigger = demo.url("/article/alpha?limit=99&retry_after=3")
    batch1 = repo.create_batch([(trigger, origin)])
    batch2 = repo.create_batch([(other, origin)])

    async def scenario():
        task = asyncio.create_task(worker.run())
        try:
            b1 = await run_until(repo, batch1, timeout=20)
            b2 = await run_until(repo, batch2, timeout=20)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return b1, b2

    b1, b2 = asyncio.run(scenario())
    assert b1["pages"][0]["status"] == "failed"
    assert b1["pages"][0]["error_code"] == "rate_limited"
    assert b2["pages"][0]["status"] == "ok"
    entries = demo.requests()
    assert entries[0]["status"] == 429
    # The trigger page is retried once after the cooldown ...
    retries = [e for e in entries[1:] if e["path"] == "/article/alpha"]
    assert len(retries) == 1
    assert retries[0]["received_at"] - entries[0]["received_at"] >= 3.0
    # ... and the other batch on the same origin also waited out the pause.
    successes = [e for e in entries if e["status"] == 200]
    assert len(successes) == 1
    assert successes[0]["received_at"] - entries[0]["received_at"] >= 3.0


def test_cancelled_batch_stops_new_requests(demo, tmp_path):
    worker, repo = make_worker(tmp_path, name="w1")
    origin = _origin(demo.url("/"))
    urls = [demo.url(f"/article/{slug}") for slug in ("alpha", "beta", "gamma", "delta")]
    batch_id = repo.create_batch([(u, origin) for u in urls])

    async def scenario():
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.sleep(0.6)  # first request is in flight
            repo.cancel_batch(batch_id)
            batch = await run_until(repo, batch_id, timeout=15)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return batch

    batch = asyncio.run(scenario())
    statuses = [p["status"] for p in batch["pages"]]
    assert statuses.count("cancelled") >= 2
    assert "ok" not in statuses or batch["pages"][0]["status"] == "ok"
    assert len([e for e in demo.requests() if e["path"].startswith("/article/")]) <= 2


def test_cancel_before_save_discards_late_body(demo, tmp_path):
    """Cancel while the body is in flight: it must not enter the corpus."""
    worker, repo = make_worker(tmp_path, name="w1")
    url = demo.url("/article/alpha?delay=1.5")
    batch_id = repo.create_batch([(url, _origin(url))])

    async def scenario():
        task = asyncio.create_task(worker.run())
        try:
            deadline = time.time() + 10
            while time.time() < deadline:
                b = repo.get_batch(batch_id)
                if b["pages"][0]["status"] == "running":
                    break
                await asyncio.sleep(0.02)
            assert repo.get_batch(batch_id)["pages"][0]["status"] == "running"
            repo.cancel_batch(batch_id)
            batch = await run_until(repo, batch_id, timeout=15)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return batch

    batch = asyncio.run(scenario())
    page = batch["pages"][0]
    assert page["status"] == "cancelled"
    assert page["title"] is None
    assert page["body_length"] == 0
    assert repo.corpus_rows(batch_id) == []


def test_429_cooldown_survives_cancel_of_triggering_batch(demo, tmp_path):
    worker, repo = make_worker(tmp_path, name="w1")
    origin = _origin(demo.url("/"))
    trigger = demo.url("/article/alpha?limit=1&retry_after=4")
    followup = demo.url("/article/beta")
    batch1 = repo.create_batch([(trigger, origin)])
    batch2 = repo.create_batch([(followup, origin)])

    async def scenario():
        task = asyncio.create_task(worker.run())
        try:
            # Wait until the 429 was received and a cooldown is active.
            deadline = time.time() + 10
            while time.time() < deadline:
                gov = OriginGovernor(str(tmp_path / "corpus.db"), owner="probe")
                if gov.cooldown_remaining(origin) > 1:
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("no cooldown observed")
            repo.cancel_batch(batch1)
            b2 = await run_until(repo, batch2, timeout=20)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return b2

    b2 = asyncio.run(scenario())
    assert b2["pages"][0]["status"] == "ok"
    entries = demo.requests()
    assert entries[0]["status"] == 429
    follow = [e for e in entries if e["status"] == 200]
    assert follow, entries
    assert follow[0]["received_at"] - entries[0]["received_at"] >= 4.0


def test_two_workers_share_pacing(demo, tmp_path):
    """Two worker processes collecting concurrently respect the shared limit.

    One page is slow, so overlap would be easy if the workers did not share
    the origin slot.
    """
    db = str(tmp_path / "corpus.db")
    repo = Repository(db)
    urls = [
        demo.url("/article/alpha?delay=1.2"),
        demo.url("/article/beta"),
        demo.url("/article/gamma"),
        demo.url("/article/delta"),
    ]
    batch_id = repo.create_batch([(u, _origin(u)) for u in urls])

    async def scenario():
        workers = []
        for name in ("w1", "w2"):
            gov = OriginGovernor(db, owner=name, min_interval=1.05)
            workers.append(Worker(repo, gov, name=name))
        tasks = [asyncio.create_task(w.run()) for w in workers]
        try:
            batch = await run_until(repo, batch_id, timeout=40)
        finally:
            for w in workers:
                w.request_stop()
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
        return batch

    batch = asyncio.run(scenario())
    assert all(p["status"] == "ok" for p in batch["pages"]), batch["pages"]
    entries = sorted(demo.requests(), key=lambda e: e["received_at"])
    assert len(entries) == 4
    for earlier, later in zip(entries, entries[1:]):
        assert later["received_at"] - earlier["received_at"] >= 1.0
        assert later["received_at"] >= earlier["finished_at"] - 0.001, "同源请求重叠"


def test_cooldown_on_one_origin_does_not_block_another(demo, tmp_path):
    """While an origin cools down after 429, other origins keep going."""
    worker, repo = make_worker(tmp_path, name="w1")
    # Two distinct origins: the same demo server reached via two host names.
    port = demo.port
    a_url = f"http://127.0.0.1:{port}/article/alpha?limit=1&retry_after=3"
    b_urls = [
        f"http://localhost:{port}/article/beta",
        f"http://localhost:{port}/article/gamma",
    ]
    a_origin = _origin(a_url)
    b_origin = _origin(b_urls[0])
    assert a_origin != b_origin
    batch_id = repo.create_batch(
        [(a_url, a_origin)] + [(url, b_origin) for url in b_urls]
    )

    async def scenario():
        task = asyncio.create_task(worker.run())
        try:
            batch = await run_until(repo, batch_id, timeout=30)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5)
        return batch

    batch = asyncio.run(scenario())
    assert [p["status"] for p in batch["pages"]] == ["ok", "ok", "ok"], batch["pages"]
    entries = sorted(demo.requests(), key=lambda e: e["received_at"])
    first_429 = [e for e in entries if e["status"] == 429][0]
    retry = [e for e in entries if e["path"] == "/article/alpha" and e["status"] == 200][0]
    assert retry["received_at"] - first_429["received_at"] >= 3.0
    for path in ("/article/beta", "/article/gamma"):
        other = [e for e in entries if e["path"] == path][0]
        # Both other-origin pages finished during origin A's cooldown.
        assert other["finished_at"] < retry["received_at"], "其他源被冷却阻塞"


def test_jsonl_export(repo_factory):
    repo = repo_factory()
    batch_id = repo.create_batch([("http://a:80/x", "http://a:80")])
    page = repo.claim_page(_first_page_id(repo, batch_id), "w")
    assert page is not None
    token = page["claim_token"]
    assert repo.start_request(page["id"], token)
    assert repo.save_result(page["id"], token, "标题", "正文\n第二段", 200) == "ok"
    rows = repo.corpus_rows(batch_id)
    assert [(r["title"], r["body"], r["url"]) for r in rows] == [
        ("标题", "正文\n第二段", "http://a:80/x")
    ]


def _first_page_id(repo: Repository, batch_id: str) -> int:
    return repo.get_batch(batch_id)["pages"][0]["id"]


@pytest.fixture()
def repo_factory(tmp_path):
    def factory() -> Repository:
        return Repository(str(tmp_path / "unit.db"))

    return factory
