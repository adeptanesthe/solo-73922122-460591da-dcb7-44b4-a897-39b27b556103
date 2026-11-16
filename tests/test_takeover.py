"""Regression tests for short-lease occupation and dead-worker takeover.

Covered interleavings (see README «进程退出与接手»):

* 1.5s lease + 4s slow response, two collectors: the response must not be
  stolen (lease renewal), requests to one origin never overlap;
* a collector that is killed mid-request: the survivor reclaims its running
  page without any restart, and the page completes;
* late writes from the dead owner (zombie) are rejected and cannot rewrite
  the reclaimed page or release the new owner's slot;
* cancelled pages are never resurrected by the takeover sweep.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import sqlite3
import time

import httpx
import pytest

from app.db import Repository
from app.fetch import normalize_url
from app.governor import OriginGovernor
from app.worker import Worker

from test_worker import DemoServer, run_until


def _origin(url: str) -> str:
    return normalize_url(url)[1]


@pytest.fixture()
def demo():
    with DemoServer() as server:
        yield server


def make_worker(db, repo, name, *, lease=1.5, dead_after=None, reclaim_interval=None):
    kwargs = {}
    if dead_after is not None:
        kwargs["dead_after"] = dead_after
    if reclaim_interval is not None:
        kwargs["reclaim_interval"] = reclaim_interval
    return Worker(
        repo,
        OriginGovernor(db, owner=name, min_interval=1.0, lease_seconds=lease),
        name=name,
        **kwargs,
    )


async def run_two_workers(workers, repo, batch_id, timeout=40.0):
    tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        return await run_until(repo, batch_id, timeout=timeout)
    finally:
        for w in workers:
            w.request_stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)


def test_short_lease_slow_source_requests_never_overlap(demo, tmp_path):
    """The reported repro: lease 1.5s, source answers after 4s, two workers.

    Before the fix the second request started as soon as the 1.5s lease
    lapsed, overlapping the 4s response.  With lease renewal the slot is held
    for the whole response and the second request waits it out.
    """
    db = str(tmp_path / "corpus.db")
    repo = Repository(db)
    urls = [demo.url("/article/alpha?delay=4"), demo.url("/article/beta")]
    batch_id = repo.create_batch([(u, _origin(u)) for u in urls])
    workers = [make_worker(db, repo, "w1"), make_worker(db, repo, "w2")]

    batch = asyncio.run(run_two_workers(workers, repo, batch_id))

    assert [p["status"] for p in batch["pages"]] == ["ok", "ok"], batch["pages"]
    entries = sorted(
        (e for e in demo.requests() if e["path"].startswith("/article/")),
        key=lambda e: e["received_at"],
    )
    assert len(entries) == 2
    first, second = entries
    assert second["received_at"] >= first["received_at"] + 0.99, "同源开始间隔不足 1 秒"
    assert second["received_at"] >= first["finished_at"] - 0.01, (
        "短租约下慢响应被抢走：同源请求发生重叠"
    )


def _doomed_worker_main(db_path: str, die_file: str) -> None:
    """Child process body: run a collector, then die abruptly on demand.

    The suicide watch uses ``os._exit`` — no cleanup, no heartbeat, no lease
    renewal, exactly like a SIGKILL — because the test sandbox does not allow
    signalling child processes.  From the database's point of view the
    process simply vanishes mid-request.
    """
    import logging
    import threading

    logging.disable(logging.CRITICAL)
    from app.db import Repository
    from app.governor import OriginGovernor
    from app.worker import Worker

    def suicide_watch() -> None:
        deadline = time.time() + 120
        while time.time() < deadline:
            if os.path.exists(die_file):
                os._exit(1)
            time.sleep(0.05)
        os._exit(2)

    threading.Thread(target=suicide_watch, daemon=True).start()
    repo = Repository(db_path)
    worker = Worker(
        repo,
        OriginGovernor(db_path, owner="doomed", min_interval=1.0, lease_seconds=1.5),
        name="doomed",
    )
    asyncio.run(worker.run())


def test_survivor_takes_over_running_page_after_process_kill(demo, tmp_path):
    """A collector dies mid-request; the survivor must pick the page up
    and finish it without any restart."""
    if not hasattr(os, "fork"):
        pytest.skip("需要 fork 来模拟进程退出")
    db = str(tmp_path / "corpus.db")
    die_file = tmp_path / "die"
    repo = Repository(db)
    url = demo.url("/article/alpha?delay=4")
    batch_id = repo.create_batch([(url, _origin(url))])

    ctx = multiprocessing.get_context("fork")
    proc = ctx.Process(target=_doomed_worker_main, args=(db, str(die_file)), daemon=True)
    proc.start()
    try:
        # Wait until the doomed process really has the request in flight.
        deadline = time.time() + 15
        while time.time() < deadline:
            if demo.requests():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("doomed 进程没有发出请求")
        page = repo.get_batch(batch_id)["pages"][0]
        assert page["status"] == "running"
        assert page["worker_id"].startswith("doomed")
        # Abrupt death: heartbeats and lease renewals stop immediately.
        die_file.touch()
        proc.join(timeout=10)
        assert proc.exitcode == 1

        survivor = make_worker(db, repo, "survivor", dead_after=1.0, reclaim_interval=0.3)
        batch = asyncio.run(run_two_workers([survivor], repo, batch_id, timeout=30))
    finally:
        die_file.touch()  # make sure the child is gone even on failure
        proc.join(timeout=10)

    page = batch["pages"][0]
    assert page["status"] == "ok", page
    assert page["worker_id"].startswith("survivor")
    rows = repo.corpus_rows(batch_id)
    assert len(rows) == 1 and rows[0]["title"]
    # The doomed process's request plus the survivor's takeover request.
    entries = [e for e in demo.requests() if e["path"] == "/article/alpha"]
    assert len(entries) == 2
    # A takeover event was recorded for the operations view.
    kinds = [e["kind"] for e in batch["events"]]
    assert "takeover" in kinds


def test_reclaimed_page_ignores_late_writes_from_dead_owner(tmp_path):
    """After a takeover, anything the old owner does late is rejected."""
    repo = Repository(str(tmp_path / "corpus.db"))
    batch_id = repo.create_batch([("http://a:80/x", "http://a:80")])
    page = repo.get_batch(batch_id)["pages"][0]

    claimed = repo.claim_page(page["id"], "dead-worker")
    old_token = claimed["claim_token"]
    assert repo.start_request(page["id"], old_token) == 1

    # The owner dies (heartbeat far in the past); a survivor reclaims the page.
    repo.worker_heartbeat("dead-worker", now=time.time() - 100)
    repo.worker_heartbeat("survivor")
    reclaimed = repo.reclaim_dead_workers(dead_after=1.0)
    assert [r["id"] for r in reclaimed] == [page["id"]]
    assert repo.get_batch(batch_id)["pages"][0]["status"] == "queued"

    reclaimed_claim = repo.claim_page(page["id"], "survivor")
    assert reclaimed_claim is not None
    new_token = reclaimed_claim["claim_token"]
    assert new_token != old_token
    assert repo.start_request(page["id"], new_token) == 2

    # The zombie wakes up and tries to store its stale body, requeue and
    # fail the page: every write is rejected, the page stays running.
    assert repo.save_result(page["id"], old_token, "僵尸标题", "僵尸正文", 200) == "stolen"
    assert repo.save_failure(page["id"], old_token, "internal", "僵尸失败") == "stolen"
    repo.unclaim_page(page["id"], old_token, "queued")
    repo.requeue_page(page["id"], old_token, 0.5, "僵尸重排")
    mid = repo.get_batch(batch_id)["pages"][0]
    assert mid["status"] == "running"
    assert mid["title"] is None

    # The legitimate owner completes the page exactly once.
    assert repo.save_result(page["id"], new_token, "真正标题", "正文", 200) == "ok"
    rows = repo.corpus_rows(batch_id)
    assert [r["title"] for r in rows] == ["真正标题"]


def test_reclaim_never_resurrects_cancelled_pages(tmp_path):
    repo = Repository(str(tmp_path / "corpus.db"))
    batch_id = repo.create_batch([("http://a:80/x", "http://a:80")])
    page = repo.get_batch(batch_id)["pages"][0]
    claimed = repo.claim_page(page["id"], "dead-worker")
    repo.start_request(page["id"], claimed["claim_token"])
    repo.worker_heartbeat("dead-worker", now=time.time() - 100)

    repo.cancel_batch(batch_id)
    reclaimed = repo.reclaim_dead_workers(dead_after=1.0)

    assert reclaimed == []
    page_now = repo.get_batch(batch_id)["pages"][0]
    assert page_now["status"] == "cancelled"
    # And a zombie body arriving after the cancel is still discarded.
    assert repo.save_result(page["id"], claimed["claim_token"], "迟到", "正文", 200) == "cancelled"
    assert repo.corpus_rows(batch_id) == []


def test_reclaim_ignores_pages_of_live_workers(tmp_path):
    repo = Repository(str(tmp_path / "corpus.db"))
    batch_id = repo.create_batch([("http://a:80/x", "http://a:80")])
    page = repo.get_batch(batch_id)["pages"][0]
    claimed = repo.claim_page(page["id"], "live-worker")
    repo.worker_heartbeat("live-worker")  # fresh heartbeat

    assert repo.reclaim_dead_workers(dead_after=1.0) == []
    assert repo.get_batch(batch_id)["pages"][0]["status"] == "running"
    # The live owner's writes still land.
    repo.start_request(page["id"], claimed["claim_token"])
    assert repo.save_result(page["id"], claimed["claim_token"], "标题", "正文", 200) == "ok"


def test_429_cooldown_survives_owner_death(demo, tmp_path):
    """A 429 cooldown triggered by a process that then disappears still
    pauses the origin for the survivor that takes the page over."""
    db = str(tmp_path / "corpus.db")
    repo = Repository(db)
    url = demo.url("/article/alpha?limit=1&retry_after=3")
    batch_id = repo.create_batch([(url, _origin(url))])

    # The first worker receives the 429 and starts the origin cooldown.
    doomed = make_worker(db, repo, "doomed")

    async def first_attempt():
        task = asyncio.create_task(doomed.run())
        try:
            end = time.time() + 15
            while time.time() < end:
                page = repo.get_batch(batch_id)["pages"][0]
                if page["status"] == "backoff":
                    return page
                await asyncio.sleep(0.05)
            raise AssertionError("没有观察到 429 backoff")
        finally:
            doomed.request_stop()
            await asyncio.wait_for(task, timeout=5)

    page = asyncio.run(first_attempt())
    assert page["retry_count"] == 1
    # The worker is gone (clean exit deregisters; a kill would simply stop
    # heartbeats).  The cooldown lives in the shared origin gate, so the
    # survivor must still wait it out before the single retry.
    survivor = make_worker(db, repo, "survivor")
    batch = asyncio.run(run_two_workers([survivor], repo, batch_id, timeout=30))
    assert batch["pages"][0]["status"] == "ok"
    entries = [e for e in demo.requests() if e["path"] == "/article/alpha"]
    assert [e["status"] for e in entries] == [429, 200]
    assert entries[1]["received_at"] - entries[0]["received_at"] >= 3.0


def test_worker_aborts_fetch_when_origin_slot_is_lost(demo, tmp_path):
    """If the lease lapsed during a stall and another process took the origin,
    the stale fetch is aborted and the page goes back to the queue."""
    db = str(tmp_path / "corpus.db")
    repo = Repository(db)
    governor = OriginGovernor(db, owner="w1", min_interval=1.0, lease_seconds=1.5)
    worker = Worker(repo, governor, name="w1")
    url = demo.url("/article/alpha?delay=4")
    batch_id = repo.create_batch([(url, _origin(url))])
    page = repo.get_batch(batch_id)["pages"][0]

    claimed = repo.claim_page(page["id"], worker.worker_id)
    token = claimed["claim_token"]
    slot = governor.try_acquire(claimed["origin"], "slot-1")
    assert slot is not None
    assert governor.mark_dispatched(slot)
    attempt = repo.start_request(page["id"], token)
    assert attempt == 1

    # Simulate the takeover: the lease lapsed and someone else holds the row.
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE origin_gate SET lease_token = 'other', lease_until = ? WHERE origin = ?",
        (time.time() + 90, claimed["origin"]),
    )
    conn.commit()
    conn.close()

    async def scenario():
        async with httpx.AsyncClient() as client:
            worker._client = client
            await worker._fetch_with_lease(claimed, slot, attempt)

    asyncio.run(scenario())

    page_now = repo.get_batch(batch_id)["pages"][0]
    assert page_now["status"] == "backoff", page_now
    assert page_now["title"] is None
    # The attempt stays counted — the request really left the process.
    assert page_now["attempts"] == 1
    assert repo.corpus_rows(batch_id) == []
