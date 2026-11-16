"""Shared per-origin request governor for crawler processes.

Two crawler processes may run at the same time (see docker-compose).  They
must behave as one polite client towards every origin:

* requests to one origin never overlap;
* two requests to the same origin start at least one second apart;
* a 429 with an integer ``Retry-After`` pauses *all* batches for that origin
  until the cooldown ends, while other origins keep working;
* a cooldown survives the batch that triggered it, even if that batch is
  cancelled right afterwards.

The governor lives in SQLite so the guarantees hold across processes.  A row
per origin stores the earliest time of the next allowed request and a
deadline.  A request slot is claimed in a single ``BEGIN IMMEDIATE``
transaction, then held until the response body has been read (or the request
failed), which is what makes requests non-overlapping.  Leases expire after
``lease_seconds`` so a crashed worker cannot block an origin forever.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

#: Minimum gap between request *starts* for one origin.
MIN_INTERVAL_SECONDS = 1.0
#: A claimed slot is released explicitly; if a worker dies, the lease expires.
LEASE_SECONDS = 90.0


@dataclass(frozen=True)
class OriginState:
    origin: str
    next_allowed_at: float
    cooldown_until: float
    last_started_at: float | None


@dataclass(frozen=True)
class Slot:
    """A held request slot; must be released exactly once."""

    origin: str
    token: str
    owner: str
    started_at: float


class OriginGovernor:
    """Cross-process, SQLite-backed per-origin request pacing."""

    def __init__(
        self,
        db_path: str,
        *,
        owner: str = "",
        min_interval: float = MIN_INTERVAL_SECONDS,
        lease_seconds: float = LEASE_SECONDS,
    ) -> None:
        self.db_path = db_path
        self.owner = owner
        self.min_interval = float(min_interval)
        self.lease_seconds = float(lease_seconds)
        self._init_db()

    # -- plumbing ---------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS origin_gate (
                    origin          TEXT PRIMARY KEY,
                    next_allowed_at REAL NOT NULL DEFAULT 0,
                    cooldown_until  REAL NOT NULL DEFAULT 0,
                    lease_token     TEXT,
                    lease_owner     TEXT,
                    lease_until     REAL NOT NULL DEFAULT 0,
                    last_started_at REAL
                )
                """
            )

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
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

    def _ensure_row(self, conn: sqlite3.Connection, origin: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO origin_gate (origin) VALUES (?)", (origin,)
        )

    def _row(self, conn: sqlite3.Connection, origin: str) -> sqlite3.Row:
        self._ensure_row(conn, origin)
        row = conn.execute(
            "SELECT * FROM origin_gate WHERE origin = ?", (origin,)
        ).fetchone()
        assert row is not None
        return row

    @staticmethod
    def _lease_active(row: sqlite3.Row, now: float) -> bool:
        return bool(row["lease_token"]) and float(row["lease_until"]) > now

    # -- public API -------------------------------------------------------
    def state(self, origin: str) -> OriginState:
        conn = self._connect()
        try:
            row = self._row(conn, origin)
            return OriginState(
                origin=origin,
                next_allowed_at=float(row["next_allowed_at"]),
                cooldown_until=float(row["cooldown_until"]),
                last_started_at=row["last_started_at"],
            )
        finally:
            conn.close()

    def cooldown_remaining(self, origin: str, now: float | None = None) -> float:
        now = time.time() if now is None else now
        st = self.state(origin)
        return max(0.0, st.cooldown_until - now)

    def readiness(self, origin: str, now: float | None = None) -> float:
        """Earliest epoch second at which a slot might be acquirable.

        Combines the inter-start spacing, a 429 cooldown and an in-flight
        request held by any process.  Used by dispatchers to skip origins that
        cannot run yet instead of churning claims.
        """
        now = time.time() if now is None else now
        conn = self._connect()
        try:
            row = self._row(conn, origin)
            ready = max(float(row["next_allowed_at"]), float(row["cooldown_until"]))
            if self._lease_active(row, now):
                ready = max(ready, float(row["lease_until"]))
            return ready
        finally:
            conn.close()

    def try_acquire(
        self, origin: str, token: str, now: float | None = None
    ) -> Slot | None:
        """Claim the next request slot for ``origin``.

        Returns ``None`` when a request is in flight, the minimum interval has
        not elapsed, or the origin is cooling down.  Never waits.

        The timestamp is taken *inside* the transaction by default: callers
        must not pass a timestamp captured before other database work, or a
        stale value could open the next slot too early.
        """
        with self._immediate() as conn:
            now = time.time() if now is None else now
            row = self._row(conn, origin)
            if self._lease_active(row, now):
                return None
            if float(row["cooldown_until"]) > now:
                return None
            if float(row["next_allowed_at"]) > now:
                return None
            lease_until = now + self.lease_seconds
            conn.execute(
                """
                UPDATE origin_gate
                   SET lease_token = ?, lease_owner = ?, lease_until = ?,
                       next_allowed_at = ?, last_started_at = ?
                 WHERE origin = ?
                """,
                (token, self.owner, lease_until, now + self.min_interval, now, origin),
            )
        return Slot(origin=origin, token=token, owner=self.owner, started_at=now)

    def mark_dispatched(self, slot: Slot, now: float | None = None) -> bool:
        """Anchor the origin's next slot at the real request dispatch time.

        Called immediately before the HTTP request is sent.  Returns ``False``
        if the slot is no longer held (the lease expired), in which case the
        caller must not send the request.
        """
        with self._immediate() as conn:
            now = time.time() if now is None else now
            row = self._row(conn, slot.origin)
            if row["lease_token"] != slot.token:
                return False
            next_allowed = max(float(row["next_allowed_at"]), now + self.min_interval)
            conn.execute(
                "UPDATE origin_gate SET last_started_at = ?, next_allowed_at = ?"
                " WHERE origin = ?",
                (now, next_allowed, slot.origin),
            )
            return True

    def release(
        self, slot: Slot, *, sent: bool = True, now: float | None = None
    ) -> None:
        """Release a slot after the request body was read (or failed).

        ``sent=False`` means no request left this process (the page turned out
        to be cancelled or another process won the claim).  The slot claimed by
        :meth:`try_acquire` reserved one interval; since nothing was sent, that
        reservation is dropped again so honest work is not delayed.

        ``sent=True`` keeps the dispatch anchor written by
        :meth:`mark_dispatched`; the next request may only start one interval
        after the previous request actually started.
        """
        with self._immediate() as conn:
            now = time.time() if now is None else now
            row = self._row(conn, slot.origin)
            if row["lease_token"] != slot.token:
                return  # lease expired and was taken over; nothing to release
            if sent:
                next_allowed = max(float(row["next_allowed_at"]), now)
            else:
                next_allowed = max(float(row["cooldown_until"]), now)
            conn.execute(
                """
                UPDATE origin_gate
                   SET lease_token = NULL, lease_owner = NULL, lease_until = 0,
                       next_allowed_at = ?
                 WHERE origin = ?
                """,
                (next_allowed, slot.origin),
            )

    def start_cooldown(
        self, origin: str, seconds: int, *, now: float | None = None
    ) -> float:
        """Pause every batch for ``origin`` for at least ``seconds``.

        The pause is anchored at the moment the 429 was observed and is
        persisted, so it remains effective after the triggering batch is
        cancelled.  Returns the cooldown deadline (epoch seconds).
        """
        if not isinstance(seconds, int) or isinstance(seconds, bool):
            raise TypeError("Retry-After must be an integer number of seconds")
        seconds = max(0, int(seconds))
        now = time.time() if now is None else now
        deadline = now + seconds
        with self._immediate() as conn:
            row = self._row(conn, origin)
            deadline = max(deadline, float(row["cooldown_until"]))
            conn.execute(
                "UPDATE origin_gate SET cooldown_until = ? WHERE origin = ?",
                (deadline, origin),
            )
        return deadline

    def clear_cooldown(self, origin: str) -> None:
        with self._immediate() as conn:
            self._row(conn, origin)
            conn.execute(
                "UPDATE origin_gate SET cooldown_until = 0 WHERE origin = ?",
                (origin,),
            )
