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
"""


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
    finally:
        conn.close()


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
        was cancelled in the meantime.  Returns the claimed page or None."""
        now = time.time() if now is None else now
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
                "UPDATE pages SET status = 'running', started_at = ?, worker_id = ?"
                " WHERE id = ? AND status = ?",
                (now, worker_id, page_id, row["status"]),
            ).rowcount
            if not updated:
                return None
            claimed = dict(row)
            claimed["status"] = "running"
            claimed["started_at"] = now
            claimed["worker_id"] = worker_id
            return claimed

    def unclaim_page(self, page_id: int, previous_status: str = "queued") -> None:
        """Return a claimed page to the queue without sending a request."""
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status = ?, started_at = NULL, worker_id = NULL"
                " WHERE id = ? AND status = 'running'",
                (previous_status, page_id),
            )

    def requeue_page(self, page_id: int, delay: float, reason: str) -> None:
        """Return a running page to the queue when no request was sent.

        The attempt counted by :meth:`start_request` is undone, so a page is
        never denied its one allowed retry because of a lease mishap.
        """
        now = time.time()
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status = 'backoff', reason = ?, error_code = NULL,"
                " next_attempt_at = ?, started_at = NULL, worker_id = NULL,"
                " attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END"
                " WHERE id = ? AND status = 'running'",
                (reason, now + delay, page_id),
            )

    def reclaim_stale(self, older_than: float) -> int:
        """Requeue pages left ``running`` by a process that died.

        A live worker holds the origin lease while its request runs, and the
        lease expires on its own; this only cleans up the database state.
        """
        cutoff = time.time() - older_than
        now = time.time()
        with transaction(self.db_path) as conn:
            rows = conn.execute(
                "SELECT id FROM pages WHERE status = 'running'"
                " AND COALESCE(started_at, 0) < ?",
                (cutoff,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE pages SET status = 'queued', started_at = NULL,"
                    " worker_id = NULL, next_attempt_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
            return len(rows)

    def cancel_claimed_page(self, page_id: int, reason: str) -> None:
        with transaction(self.db_path) as conn:
            conn.execute(
                "UPDATE pages SET status='cancelled', error_code='cancelled',"
                " reason=?, finished_at=? WHERE id=? AND status='running'",
                (reason, time.time(), page_id),
            )

    def start_request(self, page_id: int, now: float | None = None) -> int | None:
        """Count one HTTP attempt and return the new attempt number.

        Returns ``None`` if the batch was cancelled — the caller must not send
        the request.
        """
        now = time.time() if now is None else now
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.attempts, b.cancelled_at FROM pages p"
                " JOIN batches b ON b.id = p.batch_id WHERE p.id = ?",
                (page_id,),
            ).fetchone()
            if row is None or row["status"] != "running":
                return None
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
    def save_result(self, page_id: int, title: str, body: str, http_status: int) -> str:
        """Store a fetched article unless the batch was cancelled first.

        Returns ``"ok"``, ``"cancelled"`` or ``"gone"``.
        """
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, p.batch_id, b.cancelled_at FROM pages p"
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
            conn.execute(
                "UPDATE pages SET status='ok', title=?, body=?, http_status=?,"
                " reason=NULL, error_code=NULL, fetched_at=?, finished_at=?"
                " WHERE id=?",
                (title, body, http_status, now, now, page_id),
            )
            return "ok"

    def save_failure(
        self,
        page_id: int,
        code: str,
        reason: str,
        http_status: int | None = None,
    ) -> str:
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, b.cancelled_at FROM pages p"
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
            conn.execute(
                "UPDATE pages SET status='failed', error_code=?, reason=?,"
                " http_status=?, finished_at=?, title=NULL, body=NULL"
                " WHERE id=?",
                (code, reason, http_status, now, page_id),
            )
            return "failed"

    def schedule_retry(
        self,
        page_id: int,
        next_attempt_at: float,
        http_status: int,
        reason: str,
    ) -> str:
        """Put a 429 page into ``backoff`` for its single retry."""
        now = time.time()
        with transaction(self.db_path) as conn:
            row = conn.execute(
                "SELECT p.status, b.cancelled_at FROM pages p"
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
            conn.execute(
                "UPDATE pages SET status='backoff', reason=?, error_code='rate_limited',"
                    " http_status=?, retry_count = retry_count + 1,"
                    " next_attempt_at=?, started_at=NULL, worker_id=NULL"
                " WHERE id=?",
                (reason, http_status, next_attempt_at, page_id),
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
