"""State manager for a single refresh_jobs row.

Wraps all reads and writes a pipeline run makes against the row: progress
updates (throttled), cancel polling, log_tail FIFO, error append, and
terminal status transitions.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from supabase import Client

log = logging.getLogger("moviemaestro.job_state")

LOG_TAIL_LINES = 50
PROGRESS_FLUSH_EVERY_ITEMS = 5
PROGRESS_FLUSH_EVERY_SECONDS = 3.0


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobCancelled(Exception):
    """Raised when the orchestrator detects the job was cancelled."""


class JobState:
    def __init__(self, supabase: Client, job_id: UUID) -> None:
        self.supabase = supabase
        self.job_id = str(job_id)
        self._progress: dict[str, dict[str, Any]] = {}
        self._errors: list[dict[str, Any]] = []
        self._log: deque[str] = deque(maxlen=LOG_TAIL_LINES)
        self._last_flush_at = 0.0
        self._items_since_flush = 0

    # --- reads ----------------------------------------------------------------

    def is_cancelled(self) -> bool:
        """One-shot poll of the job's status. Cheap (PK lookup)."""
        resp = (
            self.supabase.table("refresh_jobs")
            .select("status")
            .eq("id", self.job_id)
            .maybe_single()
            .execute()
        )
        if resp is None or not getattr(resp, "data", None):
            # Row vanished — treat as cancelled so we exit cleanly.
            return True
        return resp.data["status"] == "cancelled"

    def another_job_running(self) -> bool:
        """Defense-in-depth single-flight check: is another job 'running'?"""
        resp = (
            self.supabase.table("refresh_jobs")
            .select("id")
            .eq("status", "running")
            .neq("id", self.job_id)
            .limit(1)
            .execute()
        )
        return bool(resp.data)

    # --- progress / phase -----------------------------------------------------

    def set_phase(self, phase: str, **initial_progress: Any) -> None:
        """Move to a new phase. Forces an immediate flush."""
        if phase not in self._progress:
            self._progress[phase] = {}
        self._progress[phase].update(initial_progress)
        self._write({"phase": phase, "progress": self._progress})
        self.append_log(f"phase → {phase}")
        self._items_since_flush = 0
        self._last_flush_at = time.monotonic()

    def update_progress(self, phase: str, **fields: Any) -> None:
        """Throttled progress update — flushes on cadence."""
        if phase not in self._progress:
            self._progress[phase] = {}
        self._progress[phase].update(fields)
        self._items_since_flush += 1
        now = time.monotonic()
        if (
            self._items_since_flush >= PROGRESS_FLUSH_EVERY_ITEMS
            or now - self._last_flush_at >= PROGRESS_FLUSH_EVERY_SECONDS
        ):
            self._flush_progress(now)

    def flush_progress(self) -> None:
        """Force a flush — call at phase boundaries and terminal transitions."""
        self._flush_progress(time.monotonic())

    def _flush_progress(self, now: float) -> None:
        self._write({"progress": self._progress})
        self._items_since_flush = 0
        self._last_flush_at = now

    # --- errors / log ---------------------------------------------------------

    def add_error(self, phase: str, item: str, error: str | Exception) -> None:
        entry = {
            "phase": phase,
            "item": item,
            "error": f"{type(error).__name__}: {error}" if isinstance(error, Exception) else str(error),
            "at": _utcnow_iso(),
        }
        self._errors.append(entry)
        self.append_log(f"ERROR {phase}/{item}: {entry['error']}")
        self._write({"errors": self._errors})

    def append_log(self, line: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        self._log.append(f"[{ts}] {line}")

    # --- terminal transitions -------------------------------------------------

    def complete(self) -> None:
        landed = self._write(
            {
                "status": "completed",
                "phase": None,
                "finished_at": _utcnow_iso(),
                "progress": self._progress,
                "errors": self._errors,
            },
            only_if_running=True,
        )
        if landed:
            log.info("job %s completed (errors=%d)", self.job_id, len(self._errors))
        else:
            log.info(
                "job %s: complete() was a no-op — row is no longer 'running' "
                "(cancellation raced ahead of the terminal write)",
                self.job_id,
            )

    def fail(self, message: str) -> None:
        self.append_log(f"FAILED: {message}")
        landed = self._write(
            {
                "status": "failed",
                "phase": None,
                "finished_at": _utcnow_iso(),
                "progress": self._progress,
                "errors": self._errors + [{"phase": "orchestrator", "item": None, "error": message, "at": _utcnow_iso()}],
            },
            only_if_running=True,
        )
        if landed:
            log.error("job %s failed: %s", self.job_id, message)
        else:
            log.info(
                "job %s: fail() was a no-op — row is no longer 'running' (cancellation raced ahead)",
                self.job_id,
            )

    def mark_cancelled(self) -> None:
        """Confirm a cancellation that was initiated externally (status flipped
        to 'cancelled' by bpdiscord). We just stamp finished_at and flush."""
        self.append_log("cancelled — stopping work")
        self._write(
            {
                "phase": None,
                "finished_at": _utcnow_iso(),
                "progress": self._progress,
                "errors": self._errors,
            }
        )
        log.info("job %s cancelled", self.job_id)

    # --- raw write ------------------------------------------------------------

    def _write(self, fields: dict[str, Any], *, only_if_running: bool = False) -> bool:
        # Always include the current log_tail snapshot.
        # only_if_running: terminal writes (complete/fail) pass this so an
        # externally-set cancellation that landed between the last is_cancelled()
        # poll and this write isn't clobbered. Returns whether the row was
        # actually updated.
        payload = {**fields, "log_tail": "\n".join(self._log), "updated_at": _utcnow_iso()}
        query = self.supabase.table("refresh_jobs").update(payload).eq("id", self.job_id)
        if only_if_running:
            query = query.eq("status", "running")
        resp = query.execute()
        return bool(getattr(resp, "data", None))
