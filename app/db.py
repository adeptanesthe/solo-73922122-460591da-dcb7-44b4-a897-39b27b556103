"""SQLite storage for batches, pages and the shared origin governor."""

from __future__ import annotations

import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

PENDING_STATUSES = ("queued", "backoff", "running")
TERMINAL_STATUSES = ("ok", "failed", "cancelled")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id            TEXT PRIMARY KEY,
    created_at    REAL NOT NULL,
    cancelled_at  REAL,
    total         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    position        INTEGER NOT NULL,
    url             TEXT NOT NULL,
    origin          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    reason          TEXT,
    error_code      TEXT,
    http_status     INTEGER,
    attempts        INTEGER NOT NULL DEFAULT 0,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    title           TEXT,
    body            TEXT,
    started_at      REAL,
    finished_at     REAL,
    fetched_at      REAL,
    worker_id       TEXT,
    claim_token     TEXT,
    UNIQUE (batch_id, position)
);

CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_pages_batch ON pages(batch_id, position);
CREATE INDEX IF NOT EXISTS idx_batches_created ON batches(created_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT NOT NULL,
    page_id    INTEGER,
    created_at REAL NOT NULL,
    kind       TEXT NOT NULL,
    message    TEXT
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id    TEXT PRIMARY KEY,
    last_seen_at REAL NOT NULL
);
"""

#: Heartbeat rows older than this are pruned (they only exist to let
#: survivors tell dead collectors from live ones).
WORKER_ROW_TTL = 3600.0


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring databases created by older versions up to date."""
    page_cols = {row[1] for row in conn.execute("PRAGMA table_info(pages)")}
    if "claim_token" not in page_cols:
        conn.execute("ALTER TABLE pages ADD COLUMN claim_token TEXT")


@contextmanager
def transaction(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


class Repository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        init_db(db_path)

    # -- batches ----------------------------------------------------------
    def create_batch(self, pages: Iterable[tuple[str, str]]) -> str:
        """``pages`` yields ``(url, origin)`` pairs."""
        items = list(pages)
        now = time.time()
        batch_id = uuid.uuid4().hex[:16]
        with transaction(self.db_path) as conn:
            conn.execute(
                "INSERT INTO batches (id, created_at, total) VALUES (?, ?, ?)",
                (batch_id, now, len(items)),
            )
            conn.executemany(
                """
                INSERT INTO pages (batch_id, position, url, origin, status)
                VALUES (?, ?, ?, ?, 'queued')
                """,
                [
                    (batch_id, pos, url, origin)
                    for pos, (url, origin) in enumerate(items)
                ],
            )
        return batch_id

    def record_event(self, batch_id: str, kind: str, message: str, page_id: int | None = None) -> None:
        try:
            conn = connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO events (batch_id, page_id, created_at, kind, message)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (batch_id, page_id, time.time(), kind, message),
                )
            finally:
                conn.close()
        except sqlite3.Error:
            pass  # event history must never break collection

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        conn = connect(self.db_path)
        try:
            batch = conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                return None
            pages = conn.execute(
                "SELECT id, batch_id, position, url, origin, status, reason, error_code,"
                "       http_status, attempts, retry_count, next_attempt_at, title,"
                "       started_at, finished_at, fetched_at, worker_id,"
                "       length(coalesce(body, '')) AS body_length"
                "  FROM pages WHERE batch_id = ? ORDER BY position",
                (batch_id,),
            ).fetchall()
            events = conn.execute(
                "SELECT created_at, kind, message FROM events WHERE batch_id = ?"
                " ORDER BY id DESC LIMIT 50",
                (batch_id,),
            ).fetchall()
            return self._batch_dict(batch, pages, events)
        finally:
            conn.close()

    def list_batches(self, limit: int = 30) -> list[dict[str, Any]]:
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            out = []
            for row in rows:
                counts = conn.execute(
                    "SELECT status, COUNT(*) AS n FROM pages WHERE batch_id = ?"
                    " GROUP BY status",
                    (row["id"],),
                ).fetchall()
                out.append(self._batch_summary(row, counts))
            return out
        finally:
            conn.close()

    def _batch_summary(self, batch: sqlite3.Row, counts_rows: Iterable[sqlite3.Row]) -> dict[str, Any]:
        counts = {s: 0 for s in PENDING_STATUSES + TERMINAL_STATUSES}
        for row in counts_rows:
            counts[row["status"]] = counts.get(row["status"], 0) + row["n"]
        total = batch["total"]
        pending = counts["queued"] + counts["backoff"] + counts["running"]
        if batch["cancelled_at"]:
            status = "cancelled"
        elif pending == 0:
            status = "completed"
        elif counts["running"] or counts["ok"] or counts["failed"]:
            status = "running"
        else:
            status = "queued"
        return {
            "id": batch["id"],
            "created_at": batch["created_at"],
            "cancelled_at": batch["cancelled_at"],
            "status": status,
            "total": total,
            "counts": counts,
            "pending": pending,
        }

    def _batch_dict(
        self,
        batch: sqlite3.Row,
        pages: Iterable[sqlite3.Row],
        events: Iterable[sqlite3.Row],
    ) -> dict[str, Any]:
        counts_rows = []
        pages_out = []
        for p in pages:
            counts_rows.append({"status": p["status"], "n": 1})
            pages_out.append(
                {
                    "id": p["id"],
                    "position": p["position"],
                    "url": p["url"],
                    "origin": p["origin"],
                    "status": p["status"],
                    "reason": p["reason"],
                    "error_code": p["error_code"],
                    "http_status": p["http_status"],
                    "attempts": p["attempts"],
                    "retry_count": p["retry_count"],
                    "next_attempt_at": p["next_attempt_at"],
                    "title": p["title"],
                    "body_length": p["body_length"],
                    "started_at": p["started_at"],
                    "finished_at": p["finished_at"],
                    "fetched_at": p["fetched_at"],
                    "worker_id": p["worker_id"],
                }
            )
        summary = self._batch_summary(batch, counts_rows)
        summary["pages"] = pages_out
        summary["events"] = [dict(e) for e in events]
        return summary

    # -- cancellation -----------------------------------------------------
    def cancel_batch(self, batch_id: str) -> dict[str, Any] | None:
        """Cancel a batch.

        Queued pages will never be requested.  A page that is already being
        fetched is marked cancelled immediately as well; the response that
        may still arrive is discarded by :meth:`save_result`, so it can never
        enter the corpus.  Nothing here touches the origin cooldown table.
        """
        now = time.time()
        with transaction(self.db_path) as conn:
            batch = conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                return None
            if batch["cancelled_at"] is None:
                conn.execute(
                    "UPDATE batches SET cancelled_at = ? WHERE id = ?",
                    (now, batch_id),
                )
                conn.execute(
                    "UPDATE pages SET status = 'cancelled',"
                    " error_code = 'cancelled',"
                    " reason = '批次已取消，未发出的请求不再发送',"
                    " finished_at = ?"
                    " WHERE batch_id = ? AND status IN ('queued', 'backoff')",
                    (now, batch_id),
                )
                conn.execute(
                    "UPDATE pages SET status = 'cancelled',"
                    " error_code = 'cancelled',"
                    " reason = '批次已取消；请求已发出，迟到的结果将被丢弃',"
                    " finished_at = ?"
                    " WHERE batch_id = ? AND status = 'running'",
                    (now, batch_id),
                )
                conn.execute(
                    "INSERT INTO events (batch_id, created_at, kind, message)"
                    " VALUES (?, ?, 'cancelled', '批次已取消')",
                    (batch_id, now),
                )
        return self.get_batch(batch_id)

    def is_cancelled(self, batch_id: str) -> bool:
        conn = connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT cancelled_at FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            return bool(row and row["cancelled_at"])
        finally:
            conn.close()

    # -- worker queue -----------------------------------------------------
    def queued_candidates(self) -> list[dict[str, Any]]:
        """All runnable pages, oldest first.  The dispatcher picks one page per
        origin."""
        now = time.time()
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                """
                SELECT p.id, p.batch_id, p.url, p.origin, p.status,
                       p.retry_count, p.next_attempt_at
                  FROM pages p
                  JOIN batches b ON b.id = p.batch_id
                 WHERE p.status IN ('queued', 'backoff')
                   AND b.cancelled_at IS NULL
                   AND (p.status = 'queued' OR p.next_attempt_at <= ?)
                 ORDER BY p.id
                """,
                (now,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def claim_page(self, page_id: int, worker_id: str, now: float | None = None) -> dict[str, Any] | None:
        """Atomically move one runnable page to ``running`` unless its batch
        was cancelled in the meantime.  Returns the claimed page or None.

        The claim carries a unique ``claim_token``: every later write the
        worker makes for this page must present that token, so a worker that
        lost the page (it was reclaimed after the owner stopped heartbeating)
        can no longer change it, even if the old process comes back to life.
        """
        now = time.time() if now is None else now
        token = uuid.uuid4().hex
        with transaction(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT p.*, b.cancelled_at
                  FROM pages p JOIN batches b ON b.id = p.batch_id
                 WHERE p.id = ?
                """,
                (page_id,),
            ).fetchone()
            if row is None:
                return None
            if row["cancelled_at"] is not None:
                if row["status"] in ("queued", "backoff"):
                    conn.execute(
                        "UPDATE pages SET status='cancelled', error_code='cancelled',"
                        " reason='批次已取消，未发出的请求不再发送', finished_at=?"
                        " WHERE id = ?",
                        (now, page_id),
                    )
                return None
            if row["status"] not in ("queued", "backoff"):
                return None
            if row["status"] == "backoff" and float(row["next_attempt_at"]) > now:
                return None
            updated = conn.execute(
                "UPDATE pages SET status = 'running', started_at = ?, worker_id = ?,"
                " claim_token = ? WHERE id = ? AND status = ?",
                (now, worker_id, token, page_id, row["status"]),
            ).rowcount
            if not updated:
                return None
            claimed = dict(row)
            claimed["status"] = "running"
            claimed["started_at"] = now
            claimed["worker_id"] = worker_id
            claimed["claim_token"] = token
            return claimed

    def unclaim_page(self, page_id: int, claim_token: str, previous_status: str = "queued") -> None:
        """Return a claimed page to the queue without sending a request.

        Only the current claim holder may do this: a stale owner (its page
        was reclaimed while it was gone) must not disturb the new claim.
        """
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status = ?, started_at = NULL, worker_id = NULL,"
                " claim_token = NULL"
                " WHERE id = ? AND status = 'running' AND claim_token = ?",
                (previous_status, page_id, claim_token),
            )

    def requeue_page(
        self,
        page_id: int,
        claim_token: str,
        delay: float,
        reason: str,
        *,
        refund_attempt: bool = True,
    ) -> None:
        """Return a running page to the queue when its fetch came to nothing.

        ``refund_attempt`` undoes the attempt counted by :meth:`start_request`
        when no request was actually sent, so a page is never denied its one
        allowed retry because of a lease mishap.  Callers whose request *did*
        leave the process (e.g. the origin slot was lost mid-flight) pass
        ``False`` — the attempt really happened and must stay counted.
        """
        now = time.time()
        refund = (
            "attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END"
            if refund_attempt
            else "attempts = attempts"
        )
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status = 'backoff', reason = ?, error_code = NULL,"
                " next_attempt_at = ?, started_at = NULL, worker_id = NULL,"
                " claim_token = NULL, " + refund +
                " WHERE id = ? AND status = 'running' AND claim_token = ?",
                (reason, now + delay, page_id, claim_token),
            )

    # -- worker liveness / takeover ---------------------------------------
    def worker_heartbeat(self, worker_id: str, now: float | None = None) -> None:
        """Publish that a collector process is alive.  Called a few times per
        second by every worker loop; survivors use it to tell dead collectors
        from live-but-slow ones."""
        now = time.time() if now is None else now
        with transaction(self.db_path) as conn:
            conn.execute(
                "INSERT INTO workers (worker_id, last_seen_at) VALUES (?, ?)"
                " ON CONFLICT(worker_id) DO UPDATE SET last_seen_at = excluded.last_seen_at",
                (worker_id, now),
            )

    def worker_gone(self, worker_id: str) -> None:
        """Forget a worker on clean shutdown (a killed process simply stops
        heartbeating and is forgotten by expiry instead)."""
        with transaction(self.db_path) as conn:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))

    def live_workers(self, dead_after: float, now: float | None = None) -> list[dict[str, Any]]:
        """All workers that heartbeated recently enough to remember, each
        flagged alive or dead.  Powers the operations view."""
        now = time.time() if now is None else now
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT worker_id, last_seen_at FROM workers ORDER BY worker_id"
            ).fetchall()
            return [
                {
                    "worker_id": row["worker_id"],
                    "last_seen_at": row["last_seen_at"],
                    "alive": float(row["last_seen_at"]) >= now - dead_after,
                }
                for row in rows
            ]
        finally:
            conn.close()

    def reclaim_dead_workers(self, dead_after: float, now: float | None = None) -> list[dict[str, Any]]:
        """Requeue ``running`` pages whose owner process is no longer alive.

        A live worker renews its origin lease and heartbeats a few times per
        second, so a page is only reclaimed when its owner has genuinely
        stopped (exited or stalled beyond ``dead_after``).  Cancelled batches
        are never touched — a cancelled page must stay cancelled.  The
        reclaim clears the claim token, which is what permanently disarms the
        dead owner: any late write or release it attempts afterwards fails
        its token check.
        """
        now = time.time() if now is None else now
        cutoff = now - dead_after
        reclaimed: list[dict[str, Any]] = []
        with transaction(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT p.id, p.batch_id, p.worker_id
                  FROM pages p
                  JOIN batches b ON b.id = p.batch_id
                 WHERE p.status = 'running'
                   AND b.cancelled_at IS NULL
                   AND (
                       p.worker_id IS NULL
                       OR p.worker_id NOT IN (
                           SELECT worker_id FROM workers WHERE last_seen_at >= ?
                       )
                   )
                """,
                (cutoff,),
            ).fetchall()
            for row in rows:
                reason = f"采集进程 {row['worker_id'] or '(未知)'} 已退出，页面重新排队等待接手"
                updated = conn.execute(
                    "UPDATE pages SET status = 'queued', started_at = NULL,"
                    " worker_id = NULL, claim_token = NULL, next_attempt_at = ?,"
                    " reason = ?, error_code = NULL"
                    " WHERE id = ? AND status = 'running'",
                    (now, reason, row["id"]),
                ).rowcount
                if updated:
                    conn.execute(
                        "INSERT INTO events (batch_id, page_id, created_at, kind, message)"
                        " VALUES (?, ?, ?, 'takeover', ?)",
                        (
                            row["batch_id"],
                            row["id"],
                            now,
                            f"采集进程 {row['worker_id'] or '(未知)'} 已退出，页面由存活进程接手",
                        ),
                    )
                    reclaimed.append(dict(row))
            conn.execute("DELETE FROM workers WHERE last_seen_at < ?", (now - WORKER_ROW_TTL,))
        return reclaimed

    def cancel_claimed_page(self, page_id: int, claim_token: str, reason: str) -> None:
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status='cancelled', error_code='cancelled',"
                " reason=?, finished_at=?"
                " WHERE id=? AND status='running' AND claim_token = ?",
                (reason, time.time(), page_id, claim_token),
            )

    def start_request(self, page_id: int, claim_token: str, now: float | None = None) -> int | None:
        """Count one HTTP attempt and return the new attempt number.

        Returns ``None`` if the batch was cancelled or the claim is no longer
        ours — in both cases the caller must not send the request.
        """
        now = time.time() if now is None else now
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.attempts, p.claim_token, b.cancelled_at FROM pages p"
                " JOIN batches b ON b.id = p.batch_id WHERE p.id = ?",
                (page_id,),
            ).fetchone()
            if row is None or row["status"] != "running":
                return None
            if row["claim_token"] != claim_token:
                return None  # reclaimed while we were away; leave it alone
            if row["cancelled_at"] is not None:
                conn.execute(
                    "UPDATE pages SET status='cancelled', error_code='cancelled',"
                    " reason='批次已取消，未发出的请求不再发送', finished_at=?"
                    " WHERE id=? AND status='running'",
                    (now, page_id),
                )
                return None
            attempts = int(row["attempts"]) + 1
            conn.execute(
                "UPDATE pages SET attempts = ?, started_at = ? WHERE id = ?",
                (attempts, now, page_id),
            )
            return attempts

    # -- results ----------------------------------------------------------
    def save_result(self, page_id: int, claim_token: str, title: str, body: str, http_status: int) -> str:
        """Store a fetched article unless the batch was cancelled first.

        Returns ``"ok"``, ``"cancelled"``, ``"stolen"`` (the page was
        reclaimed and now belongs to another worker — the late body is
        dropped) or ``"gone"``.
        """
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.batch_id, p.claim_token, b.cancelled_at FROM pages p"
                " JOIN batches b ON b.id = p.batch_id WHERE p.id = ?",
                (page_id,),
            ).fetchone()
            if row is None:
                return "gone"
            if row["cancelled_at"] is not None:
                # The batch was cancelled while this response was in flight:
                # drop the body and say so on the page.
                conn.execute(
                    "UPDATE pages SET status='cancelled', error_code='cancelled',"
                    " reason='批次已取消，迟到正文已丢弃，未写入语料',"
                    " title=NULL, body=NULL, http_status=?, finished_at=?"
                    " WHERE id=? AND status IN ('running', 'cancelled')",
                    (http_status, now, page_id),
                )
                conn.execute(
                    "INSERT INTO events (batch_id, page_id, created_at, kind, message)"
                    " VALUES (?, ?, ?, 'discarded', '取消后到达的正文已丢弃')",
                    (row["batch_id"], page_id, now),
                )
                return "cancelled"
            if row["status"] != "running":
                return "gone"
            if row["claim_token"] != claim_token:
                # The page was reclaimed by a surviving process while this
                # request was in flight; its late body must not overwrite the
                # new owner's work.
                return "stolen"
            conn.execute(
                "UPDATE pages SET status='ok', title=?, body=?, http_status=?,"
                " reason=NULL, error_code=NULL, fetched_at=?, finished_at=?"
                " WHERE id=? AND status='running' AND claim_token = ?",
                (title, body, http_status, now, now, page_id, claim_token),
            )
            return "ok"

    def save_failure(
        self,
        page_id: int,
        claim_token: str,
        code: str,
        reason: str,
        http_status: int | None = None,
    ) -> str:
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.claim_token, b.cancelled_at FROM pages p"
                " JOIN batches b ON b.id = p.batch_id WHERE p.id = ?",
                (page_id,),
            ).fetchone()
            if row is None or row["status"] != "running":
                return "gone"
            if row["cancelled_at"] is not None:
                conn.execute(
                    "UPDATE pages SET status='cancelled', error_code='cancelled',"
                    " reason='批次已取消，未写入语料', http_status=?, finished_at=?"
                    " WHERE id=?",
                    (http_status, now, page_id),
                )
                return "cancelled"
            if row["claim_token"] != claim_token:
                return "stolen"
            conn.execute(
                "UPDATE pages SET status='failed', error_code=?, reason=?,"
                " http_status=?, finished_at=?, title=NULL, body=NULL"
                " WHERE id=? AND status='running' AND claim_token = ?",
                (code, reason, http_status, now, page_id, claim_token),
            )
            return "failed"

    def schedule_retry(
        self,
        page_id: int,
        claim_token: str,
        next_attempt_at: float,
        http_status: int,
        reason: str,
    ) -> str:
        """Put a 429 page into ``backoff`` for its single retry."""
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.claim_token, b.cancelled_at FROM pages p"
                " JOIN batches b ON b.id = p.batch_id WHERE p.id = ?",
                (page_id,),
            ).fetchone()
            if row is None or row["status"] != "running":
                return "gone"
            if row["cancelled_at"] is not None:
                conn.execute(
                    "UPDATE pages SET status='cancelled', error_code='cancelled',"
                    " reason='批次已取消，不再重试', http_status=?, finished_at=?"
                    " WHERE id=?",
                    (http_status, now, page_id),
                )
                return "cancelled"
            if row["claim_token"] != claim_token:
                return "stolen"
            conn.execute(
                "UPDATE pages SET status='backoff', reason=?, error_code='rate_limited',"
                    " http_status=?, retry_count = retry_count + 1,"
                    " next_attempt_at=?, started_at=NULL, worker_id=NULL, claim_token=NULL"
                " WHERE id=? AND status='running' AND claim_token = ?",
                (reason, http_status, next_attempt_at, page_id, claim_token),
            )
            return "backoff"

    # -- corpus -----------------------------------------------------------
    def corpus_rows(self, batch_id: str) -> list[sqlite3.Row]:
        conn = connect(self.db_path)
        try:
            return conn.execute(
                "SELECT position, url, title, body, fetched_at FROM pages"
                " WHERE batch_id = ? AND status = 'ok' ORDER BY position",
                (batch_id,),
            ).fetchall()
        finally:
            conn.close()
