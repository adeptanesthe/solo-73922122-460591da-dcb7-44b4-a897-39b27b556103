"""The collection worker used by both crawler processes.

A worker loop repeatedly:

1. reads runnable pages (shared by every process through SQLite),
2. for each origin picks one page whose origin is currently free, claims it
   and acquires the shared origin slot,
3. checks the batch is still live *after* acquiring the slot (so a request is
   never sent for a cancelled batch), sends exactly one GET,
4. stores the result only if the batch was not cancelled while the request
   was in flight.

All waiting is per-origin and asynchronous, so a cooldown on one source never
blocks the other source.  A page is retried at most once, and only after a
429 with an integer ``Retry-After``.
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


class Worker:
    def __init__(
        self,
        repo: Repository,
        governor: OriginGovernor,
        *,
        name: str = "worker",
        retry_after_fallback: int = 2,
        max_retry_after: int | None = None,
    ) -> None:
        self.repo = repo
        self.governor = governor
        self.name = name
        self.worker_id = f"{name}-{uuid.uuid4().hex[:8]}"
        self.retry_after_fallback = retry_after_fallback
        self.max_retry_after = max_retry_after
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._client: httpx.AsyncClient | None = None

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
                self.repo.reclaim_stale(self.governor.lease_seconds)
            except Exception:
                log.exception("stale page cleanup failed")
            while not self._stop.is_set():
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
            if self._tasks:
                await asyncio.gather(*self._tasks.values(), return_exceptions=True)

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
                self.repo.unclaim_page(page["id"], page["status"])
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
        origin = page["origin"]
        batch_id = page["batch_id"]
        sent = False
        try:
            # Re-check under the slot: the batch may have been cancelled after
            # the claim, in which case no request may be sent at all.
            if self.repo.is_cancelled(batch_id):
                self.repo.cancel_claimed_page(page_id, "批次已取消，未发出的请求不再发送")
                return
            # Anchor the shared spacing at the moment we really dispatch, so
            # the next request (in either process) starts >= 1s later.
            if not self.governor.mark_dispatched(slot):
                # Our lease expired (this process stalled): do not send.
                self.repo.requeue_page(page_id, 0.5, "等待源站空闲后重试")
                return
            # Last check before the network: a cancel that happened after the
            # slot was acquired must still prevent this request.
            attempt = self.repo.start_request(page_id)
            if attempt is None:
                return
            sent = True
            await self._fetch_and_store(page, origin, attempt)
        except asyncio.CancelledError:
            self.repo.unclaim_page(page_id, "queued")
            raise
        except Exception as exc:
            log.exception("page %s failed unexpectedly", page_id)
            self.repo.save_failure(page_id, "internal", f"内部错误：{exc.__class__.__name__}")
        finally:
            self.governor.release(slot, sent=sent)
            self._wake.set()

    async def _fetch_and_store(self, page: dict, origin: str, attempt: int) -> None:
        assert self._client is not None
        page_id = page["id"]
        batch_id = page["batch_id"]
        url = page["url"]
        try:
            outcome = await fetchmod.fetch_article(self._client, url)
        except fetchmod.TooManyRequests as exc:
            self._handle_429(page, origin, exc, attempt)
            return
        except fetchmod.FetchProblem as exc:
            status = self.repo.save_failure(page_id, exc.code, exc.reason, exc.http_status)
            if status == "failed":
                self.repo.record_event(batch_id, "failed", f"{url}：{exc.reason}", page_id)
            return

        status = self.repo.save_result(page_id, outcome.title, outcome.body, outcome.http_status)
        if status == "cancelled":
            log.info("late body for cancelled batch %s page %s discarded", batch_id, page_id)
        elif status == "ok":
            self.repo.record_event(batch_id, "ok", f"{url}：已采集", page_id)

    def _handle_429(
        self, page: dict, origin: str, exc: fetchmod.TooManyRequests, attempt: int
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
                deadline,
                exc.http_status or 429,
                f"源站限流（429，{note}），冷却结束后重试一次",
            )
        else:
            self.repo.save_failure(
                page_id,
                "rate_limited",
                f"源站限流（429，{note}），重试后仍被拒绝",
                exc.http_status or 429,
            )
