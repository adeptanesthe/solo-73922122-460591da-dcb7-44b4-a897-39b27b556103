"""The collection worker used by both crawler processes.

A worker loop repeatedly:

1. heartbeats so survivors can tell it is alive, and requeues pages left
   ``running`` by processes that stopped heartbeating (takeover),
2. reads runnable pages (shared by every process through SQLite),
3. for each origin picks one page whose origin is currently free, claims it
   and acquires the shared origin slot,
4. checks the batch is still live *after* acquiring the slot (so a request is
   never sent for a cancelled batch), sends exactly one GET,
5. stores the result only if the batch was not cancelled while the request
   was in flight.

All waiting is per-origin and asynchronous, so a cooldown on one source never
blocks the other source.  A page is retried at most once, and only after a
429 with an integer ``Retry-After``.

Slow sources and leases
-----------------------

The origin slot lease is intentionally short.  While a request is in flight
the worker *renews* the lease (see :meth:`Worker._fetch_with_lease`), so a
slow but healthy response is never stolen by the other process — no matter
how much longer than the lease it takes.  If a process exits, its renewals
and heartbeats stop; the lease lapses on its own and a survivor reclaims the
orphaned page after ``dead_after`` seconds.  Every page claim carries a
unique token, so anything the dead process does late (storing a body,
releasing a slot, requeueing) is rejected and cannot disturb the new owner.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid

import httpx

from . import fetch as fetchmod
from .db import Repository
from .governor import OriginGovernor, Slot

log = logging.getLogger("collector.worker")

#: How long a dispatcher sleeps when every origin is blocked.
IDLE_SLEEP = 0.25
#: How often the worker proves it is alive.
HEARTBEAT_INTERVAL = 0.5
#: How often the worker looks for pages orphaned by dead processes.
RECLAIM_INTERVAL = 1.0


def default_dead_after(lease_seconds: float) -> float:
    """How long a worker may be silent before survivors take its pages over.

    Long enough that a healthy worker (heartbeating every
    ``HEARTBEAT_INTERVAL``) is never mistaken for dead, short enough that a
    killed process is noticed within a few seconds.
    """
    return min(30.0, max(2.0, 2.0 * lease_seconds))


class Worker:
    def __init__(
        self,
        repo: Repository,
        governor: OriginGovernor,
        *,
        name: str = "worker",
        retry_after_fallback: int = 2,
        max_retry_after: int | None = None,
        dead_after: float | None = None,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
        reclaim_interval: float = RECLAIM_INTERVAL,
    ) -> None:
        self.repo = repo
        self.governor = governor
        self.name = name
        self.worker_id = f"{name}-{uuid.uuid4().hex[:8]}"
        self.retry_after_fallback = retry_after_fallback
        self.max_retry_after = max_retry_after
        self.dead_after = (
            float(dead_after)
            if dead_after is not None
            else default_dead_after(governor.lease_seconds)
        )
        self.heartbeat_interval = float(heartbeat_interval)
        self.reclaim_interval = float(reclaim_interval)
        #: Renew the origin lease well before it can lapse.
        self.renew_interval = min(10.0, max(0.2, governor.lease_seconds / 3.0))
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._client: httpx.AsyncClient | None = None
        self._last_heartbeat = 0.0
        self._last_reclaim = 0.0

    # -- lifecycle --------------------------------------------------------
    async def run(self) -> None:
        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=fetchmod.TIMEOUT,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
        ) as client:
            self._client = client
            log.info("worker %s started (pid=%s)", self.worker_id, os.getpid())
            try:
                while not self._stop.is_set():
                    self._housekeeping()
                    try:
                        worked = await self.tick()
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # keep the loop alive
                        log.exception("worker tick failed")
                        worked = False
                    if not worked:
                        try:
                            await asyncio.wait_for(self._wake.wait(), timeout=IDLE_SLEEP)
                        except asyncio.TimeoutError:
                            pass
                        self._wake.clear()
            finally:
                # Graceful exit: let in-flight requests finish (heartbeating
                # so nobody takes them over), then deregister.  A killed
                # process skips all of this and is noticed by expiry.
                if self._tasks:
                    await self._finish_inflight()
                try:
                    self.repo.worker_gone(self.worker_id)
                except Exception:
                    log.exception("worker deregistration failed")

    def _housekeeping(self) -> None:
        now = time.time()
        if now - self._last_heartbeat >= self.heartbeat_interval:
            try:
                self.repo.worker_heartbeat(self.worker_id, now)
            except Exception:
                log.exception("worker heartbeat failed")
            self._last_heartbeat = now
        if now - self._last_reclaim >= self.reclaim_interval:
            try:
                reclaimed = self.repo.reclaim_dead_workers(self.dead_after, now)
                if reclaimed:
                    log.info(
                        "took over %d page(s) from dead collectors: %s",
                        len(reclaimed),
                        ", ".join(sorted({r["worker_id"] or "?" for r in reclaimed})),
                    )
            except Exception:
                log.exception("dead worker reclaim failed")
            self._last_reclaim = now

    async def _finish_inflight(self) -> None:
        pending = [t for t in self._tasks.values() if not t.done()]
        if not pending:
            return
        gather = asyncio.gather(*pending, return_exceptions=True)
        while True:
            try:
                await asyncio.wait_for(asyncio.shield(gather), timeout=self.heartbeat_interval)
                return
            except asyncio.TimeoutError:
                try:
                    self.repo.worker_heartbeat(self.worker_id)
                except Exception:
                    log.exception("worker heartbeat failed")

    def request_stop(self) -> None:
        self._stop.set()
        self._wake.set()

    # -- scheduling -------------------------------------------------------
    async def tick(self) -> bool:
        """Start as many in-flight page requests as the origins allow."""
        candidates = self.repo.queued_candidates()
        if not candidates:
            return False

        by_origin: dict[str, dict] = {}
        for page in candidates:
            # One request at a time per origin within this process too; the
            # governor enforces it across processes.
            by_origin.setdefault(page["origin"], page)

        started = False
        for origin, page in by_origin.items():
            if self._origin_inflight(origin):
                continue
            if self.governor.readiness(origin) > time.time():
                continue
            claimed = self.repo.claim_page(page["id"], self.worker_id)
            if claimed is None:
                continue
            slot = self.governor.try_acquire(origin, uuid.uuid4().hex)
            if slot is None:
                # Another process took the origin between our two checks.
                self.repo.unclaim_page(page["id"], claimed["claim_token"], page["status"])
                continue
            task = asyncio.create_task(
                self._process(claimed, slot), name=f"{self.worker_id}:{origin}"
            )
            self._tasks[task.get_name()] = task
            task.add_done_callback(lambda t: self._tasks.pop(t.get_name(), None))
            started = True
        return started

    def _origin_inflight(self, origin: str) -> bool:
        suffix = f":{origin}"
        return any(
            not t.done() and t.get_name().endswith(suffix) for t in self._tasks.values()
        )

    async def _process(self, page: dict, slot: Slot) -> None:
        page_id = page["id"]
        token = page["claim_token"]
        origin = page["origin"]
        batch_id = page["batch_id"]
        sent = False
        try:
            # Re-check under the slot: the batch may have been cancelled after
            # the claim, in which case no request may be sent at all.
            if self.repo.is_cancelled(batch_id):
                self.repo.cancel_claimed_page(page_id, token, "批次已取消，未发出的请求不再发送")
                return
            # Anchor the shared spacing at the moment we really dispatch, so
            # the next request (in either process) starts >= 1s later.
            if not self.governor.mark_dispatched(slot):
                # Our lease expired (this process stalled): do not send.
                self.repo.requeue_page(page_id, token, 0.5, "等待源站空闲后重试")
                return
            # Last check before the network: a cancel that happened after the
            # slot was acquired must still prevent this request.
            attempt = self.repo.start_request(page_id, token)
            if attempt is None:
                return
            sent = True
            await self._fetch_with_lease(page, slot, attempt)
        except asyncio.CancelledError:
            self.repo.unclaim_page(page_id, token, "queued")
            raise
        except Exception as exc:
            log.exception("page %s failed unexpectedly", page_id)
            self.repo.save_failure(page_id, token, "internal", f"内部错误：{exc.__class__.__name__}")
        finally:
            self.governor.release(slot, sent=sent)
            self._wake.set()

    async def _fetch_with_lease(self, page: dict, slot: Slot, attempt: int) -> None:
        """Run the fetch while keeping the origin lease alive.

        A slow response may take far longer than the lease; renewing proves
        this process is alive and the slot must not be stolen.  If a renewal
        fails, the slot is already owned by someone else (this process was
        stalled beyond the lease): the in-flight request is aborted, nothing
        is stored, and the page is handed back for recollection.
        """
        page_id = page["id"]
        token = page["claim_token"]
        fetch_task = asyncio.create_task(
            self._fetch_and_store(page, page["origin"], attempt, token),
            name=f"fetch:{page_id}",
        )
        try:
            while True:
                done, _ = await asyncio.wait({fetch_task}, timeout=self.renew_interval)
                if done:
                    break
                try:
                    renewed = self.governor.renew(slot)
                except Exception:
                    # Transient storage error: the fetch is bounded by the
                    # HTTP timeout, so simply try again on the next tick.
                    log.exception("lease renewal failed transiently (page %s)", page_id)
                    continue
                if renewed:
                    continue
                log.warning(
                    "lost origin slot %s mid-request (page %s); aborting stale fetch",
                    slot.origin,
                    page_id,
                )
                await self._cancel_and_drain(fetch_task)
                # Hand the page back only if we still own the claim; if it
                # was already reclaimed, the new owner is in charge.  The
                # attempt stays counted: the request really left this process.
                self.repo.requeue_page(
                    page_id,
                    token,
                    0.5,
                    "源站槽位易主，页面重新排队等待接手",
                    refund_attempt=False,
                )
                return
            if fetch_task.cancelled():
                return
            exc = fetch_task.exception()
            if exc is not None:
                raise exc
        except asyncio.CancelledError:
            await self._cancel_and_drain(fetch_task)
            raise

    @staticmethod
    async def _cancel_and_drain(task: asyncio.Task[None]) -> None:
        """Cancel a fetch task and wait for it, absorbing its outcome."""
        if task.done():
            return
        task.cancel()
        try:
            await task
        except BaseException:
            pass

    async def _fetch_and_store(self, page: dict, origin: str, attempt: int, token: str) -> None:
        assert self._client is not None
        page_id = page["id"]
        batch_id = page["batch_id"]
        url = page["url"]
        try:
            outcome = await fetchmod.fetch_article(self._client, url)
        except fetchmod.TooManyRequests as exc:
            self._handle_429(page, origin, exc, attempt, token)
            return
        except fetchmod.FetchProblem as exc:
            status = self.repo.save_failure(page_id, token, exc.code, exc.reason, exc.http_status)
            if status == "failed":
                self.repo.record_event(batch_id, "failed", f"{url}：{exc.reason}", page_id)
            return

        status = self.repo.save_result(page_id, token, outcome.title, outcome.body, outcome.http_status)
        if status == "cancelled":
            log.info("late body for cancelled batch %s page %s discarded", batch_id, page_id)
        elif status == "stolen":
            log.info("late body for reclaimed page %s discarded (taken over)", page_id)
        elif status == "ok":
            self.repo.record_event(batch_id, "ok", f"{url}：已采集", page_id)

    def _handle_429(
        self, page: dict, origin: str, exc: fetchmod.TooManyRequests, attempt: int, token: str
    ) -> None:
        page_id = page["id"]
        batch_id = page["batch_id"]
        retry_after = exc.retry_after_seconds()
        seconds = self.retry_after_fallback if retry_after is None else retry_after
        if self.max_retry_after is not None:
            seconds = min(seconds, self.max_retry_after)
        seconds = max(0, seconds)
        deadline = self.governor.start_cooldown(origin, seconds)
        note = (
            f"Retry-After={retry_after}s"
            if retry_after is not None
            else f"Retry-After 缺失或不是整数，按 {seconds}s 处理"
        )
        self.repo.record_event(
            batch_id,
            "rate_limited",
            f"{origin} 触发 429，该源暂停 {seconds}s（{note}）",
            page_id,
        )
        if attempt <= 1:
            self.repo.schedule_retry(
                page_id,
                token,
                deadline,
                exc.http_status or 429,
                f"源站限流（429，{note}），冷却结束后重试一次",
            )
        else:
            self.repo.save_failure(
                page_id,
                token,
                "rate_limited",
                f"源站限流（429，{note}），重试后仍被拒绝",
                exc.http_status or 429,
            )
