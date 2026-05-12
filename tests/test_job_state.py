import time
from uuid import uuid4

import pytest

from app.pipeline import job_state
from app.pipeline.job_state import JobState
from tests.fakes import FakeSupabase

JOB_ID = uuid4()


@pytest.fixture
def sb() -> FakeSupabase:
    s = FakeSupabase()
    s.insert_refresh_job(JOB_ID, status="running")
    return s


def _update_payloads(sb: FakeSupabase) -> list[dict]:
    return [w["payload"] for w in sb.writes if w["op"] == "update" and w["table"] == "refresh_jobs"]


class TestCancelDetection:
    def test_returns_false_for_running(self, sb):
        s = JobState(sb, JOB_ID)
        assert s.is_cancelled() is False

    def test_returns_true_for_cancelled(self, sb):
        sb.set_refresh_job_status(JOB_ID, "cancelled")
        s = JobState(sb, JOB_ID)
        assert s.is_cancelled() is True

    def test_returns_true_when_row_missing(self, sb):
        s = JobState(sb, uuid4())  # unknown id
        # Defensive: a missing row means someone deleted it — treat as cancel.
        assert s.is_cancelled() is True


class TestAnotherJobRunning:
    def test_false_when_only_self_running(self, sb):
        s = JobState(sb, JOB_ID)
        assert s.another_job_running() is False

    def test_true_when_another_job_running(self, sb):
        other = uuid4()
        sb.insert_refresh_job(other, status="running")
        s = JobState(sb, JOB_ID)
        assert s.another_job_running() is True

    def test_false_when_other_jobs_are_completed(self, sb):
        sb.insert_refresh_job(uuid4(), status="completed")
        sb.insert_refresh_job(uuid4(), status="cancelled")
        s = JobState(sb, JOB_ID)
        assert s.another_job_running() is False


class TestProgressThrottling:
    def test_set_phase_writes_immediately(self, sb):
        s = JobState(sb, JOB_ID)
        s.set_phase("user_scrape", total=10)
        payloads = _update_payloads(sb)
        assert any("phase" in p and p["phase"] == "user_scrape" for p in payloads)

    def test_update_below_threshold_does_not_flush(self, sb):
        s = JobState(sb, JOB_ID)
        s.set_phase("user_scrape", total=10)
        sb.writes.clear()
        s.update_progress("user_scrape", processed=1)
        s.update_progress("user_scrape", processed=2)
        s.update_progress("user_scrape", processed=3)
        s.update_progress("user_scrape", processed=4)
        # 4 items < 5 threshold; no time has passed; no flush.
        assert sb.writes == []

    def test_update_flushes_after_5_items(self, sb):
        s = JobState(sb, JOB_ID)
        s.set_phase("user_scrape", total=10)
        sb.writes.clear()
        for i in range(5):
            s.update_progress("user_scrape", processed=i + 1)
        # The 5th call triggers a flush.
        assert len(sb.writes) == 1

    def test_update_flushes_after_3_seconds(self, sb, monkeypatch):
        s = JobState(sb, JOB_ID)
        s.set_phase("user_scrape", total=10)
        sb.writes.clear()
        # Capture real monotonic before patching to avoid recursion in our shim.
        real_monotonic = job_state.time.monotonic
        s.update_progress("user_scrape", processed=1)
        baseline = real_monotonic()
        monkeypatch.setattr(job_state.time, "monotonic", lambda: baseline + 4.0)
        s.update_progress("user_scrape", processed=2)
        assert len(sb.writes) >= 1

    def test_flush_progress_forces_write(self, sb):
        s = JobState(sb, JOB_ID)
        s.set_phase("user_scrape", total=10)
        sb.writes.clear()
        s.update_progress("user_scrape", processed=1)
        s.flush_progress()
        assert any(w["op"] == "update" for w in sb.writes)


class TestLogTail:
    def test_keeps_only_last_50_lines(self, sb):
        s = JobState(sb, JOB_ID)
        for i in range(60):
            s.append_log(f"line {i}")
        s.flush_progress()
        row = sb.get_refresh_job(JOB_ID)
        lines = row["log_tail"].split("\n")
        assert len(lines) == 50
        # Last 50 of 60 preserved → lines 10..59. Earliest 10 dropped.
        assert "line 59" in lines[-1]
        assert "line 10" in lines[0]
        assert not any("line 9 " in l or l.endswith("line 9") for l in lines)


class TestErrors:
    def test_add_error_appends_to_errors_array(self, sb):
        s = JobState(sb, JOB_ID)
        s.add_error("user_scrape", "alice", ValueError("nope"))
        row = sb.get_refresh_job(JOB_ID)
        assert len(row["errors"]) == 1
        assert row["errors"][0]["item"] == "alice"
        assert "ValueError" in row["errors"][0]["error"]
        assert row["errors"][0]["phase"] == "user_scrape"

    def test_add_error_writes_log_line_too(self, sb):
        s = JobState(sb, JOB_ID)
        s.add_error("film_ratings", "parasite", "boom")
        row = sb.get_refresh_job(JOB_ID)
        assert "ERROR film_ratings/parasite" in row["log_tail"]


class TestTerminalStates:
    def test_complete_sets_status_and_finished_at(self, sb):
        s = JobState(sb, JOB_ID)
        s.complete()
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        assert row["finished_at"] is not None
        assert row["phase"] is None

    def test_fail_sets_status_failed_and_records_message(self, sb):
        s = JobState(sb, JOB_ID)
        s.fail("DB unreachable")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        # The message is appended to errors[] as a synthetic orchestrator-phase entry.
        assert any("DB unreachable" in e["error"] for e in row["errors"])

    def test_mark_cancelled_does_not_change_status(self, sb):
        # Cancellation flips status externally; we just stamp finished_at.
        sb.set_refresh_job_status(JOB_ID, "cancelled")
        s = JobState(sb, JOB_ID)
        s.mark_cancelled()
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "cancelled"
        assert row["finished_at"] is not None

    def test_complete_is_noop_when_row_already_cancelled(self, sb):
        # Race: admin cancels between last is_cancelled() poll and complete().
        # The terminal write must not overwrite the cancelled status.
        sb.set_refresh_job_status(JOB_ID, "cancelled")
        s = JobState(sb, JOB_ID)
        s.complete()
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "cancelled"
        # The guarded update should match zero rows.
        update_writes = [w for w in sb.writes if w["op"] == "update"]
        assert update_writes and update_writes[-1]["matched"] == 0

    def test_fail_is_noop_when_row_already_cancelled(self, sb):
        # Same race as above, but for the failure path.
        sb.set_refresh_job_status(JOB_ID, "cancelled")
        s = JobState(sb, JOB_ID)
        s.fail("boom")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "cancelled"
        update_writes = [w for w in sb.writes if w["op"] == "update"]
        assert update_writes and update_writes[-1]["matched"] == 0
