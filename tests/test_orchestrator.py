"""End-to-end orchestrator tests against the FakeSupabase + monkeypatched leaves.

Covers the bd-7bh acceptance criteria:
- All 3 phases run in order, each updates progress.
- Mid-flight cancel exits within ~10s (we drive synchronously; the assertion is
  that we exit AT all, with finished_at stamped and not all items processed).
- Per-item failures append to errors[] and don't abort the run.
- Defense-in-depth: another running job → fail before doing any work.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from app.pipeline import film_ratings, letterboxd_throttle, orchestrator, user_films
from tests.fakes import FakeSupabase

JOB_ID = uuid4()


@pytest.fixture
def sb() -> FakeSupabase:
    s = FakeSupabase()
    s.insert_refresh_job(JOB_ID, status="running")
    s.insert_users("alice", "bob", is_discord=True)
    s.rpcs["get_missing_films"] = lambda: ["parasite", "the-room"]
    return s


def _stub_letterboxdpy(monkeypatch, *, user_films_per_user: dict | None = None):
    """Patch letterboxdpy at the modules' import sites with friendly fakes."""
    user_films_per_user = user_films_per_user or {}

    class FakeUser:
        def __init__(self, lbusername):
            self.lbusername = lbusername

        def get_films(self):
            return user_films_per_user.get(self.lbusername, {"movies": {}})

    class FakeMovie:
        def __init__(self, slug):
            self.slug = slug
            self.url = f"https://letterboxd.com/film/{slug}/"
            self.title = slug
            self.rating = 4.0
            self.tmdb_link = None
            self.poster = None
            self.banner = None

    monkeypatch.setattr(user_films, "User", FakeUser)
    monkeypatch.setattr(film_ratings, "Movie", FakeMovie)


class TestHappyPath:
    @pytest.mark.asyncio
    async def test_full_run_marks_completed(self, sb, monkeypatch):
        _stub_letterboxdpy(
            monkeypatch,
            user_films_per_user={
                "alice": {"movies": {"parasite": {"rating": 4.5, "liked": True, "name": "Parasite"}}},
                "bob": {"movies": {"the-room": {"rating": 0.5, "liked": False, "name": "The Room"}}},
            },
        )
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        assert row["finished_at"] is not None
        assert row["phase"] is None
        # All 3 phases left progress.
        assert "user_scrape" in row["progress"]
        assert "missing_films" in row["progress"]
        assert "film_ratings" in row["progress"]
        # Both users' films were upserted.
        assert len(sb.tables["UserFilms"]) == 2
        # Both missing films were upserted.
        assert len(sb.tables["Films"]) == 2

    @pytest.mark.asyncio
    async def test_progress_counters_advance(self, sb, monkeypatch):
        _stub_letterboxdpy(
            monkeypatch,
            user_films_per_user={
                "alice": {"movies": {"parasite": {"rating": 4.5, "liked": True, "name": "Parasite"}}},
                "bob": {"movies": {}},
            },
        )
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["progress"]["user_scrape"]["processed"] == 2
        assert row["progress"]["user_scrape"]["total"] == 2
        assert row["progress"]["missing_films"]["count"] == 2
        assert row["progress"]["film_ratings"]["processed"] == 2
        assert row["progress"]["film_ratings"]["total"] == 2

    @pytest.mark.asyncio
    async def test_phase_order_in_log(self, sb, monkeypatch):
        _stub_letterboxdpy(monkeypatch)
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        log = row["log_tail"]
        i_user = log.find("phase → user_scrape")
        i_missing = log.find("phase → missing_films")
        i_ratings = log.find("phase → film_ratings")
        assert -1 < i_user < i_missing < i_ratings


class TestCancellation:
    @pytest.mark.asyncio
    async def test_cancel_before_user_phase_exits_immediately(self, sb, monkeypatch):
        _stub_letterboxdpy(monkeypatch)
        sb.set_refresh_job_status(JOB_ID, "cancelled")
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "cancelled"  # untouched, just stamped
        assert row["finished_at"] is not None

    @pytest.mark.asyncio
    async def test_cancel_during_film_ratings_phase(self, sb, monkeypatch):
        # 5 films to scrape, cancel after the 2nd.
        sb.rpcs["get_missing_films"] = lambda: [f"slug-{i}" for i in range(5)]
        scraped = []

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                scraped.append(slug)
                self.slug = slug
                self.url = ""
                self.title = slug
                self.rating = 4.0
                self.tmdb_link = None
                self.poster = None
                self.banner = None
                if len(scraped) == 2:
                    sb.set_refresh_job_status(JOB_ID, "cancelled")

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        # Bailed mid-phase — not all 5 slugs processed.
        assert len(scraped) < 5
        assert row["status"] == "cancelled"
        assert row["finished_at"] is not None


class TestTombstoneOn404:
    @pytest.mark.asyncio
    async def test_404_slug_tombstoned_not_errored(self, sb, monkeypatch):
        from letterboxdpy.core.exceptions import MovieNotFoundError

        sb.rpcs["get_missing_films"] = lambda: ["good", "ghost", "ugly"]

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                if slug == "ghost":
                    raise MovieNotFoundError(slug, f"https://letterboxd.com/film/{slug}/")
                self.slug, self.url, self.title, self.rating = slug, "u", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        # 404'd slug must not pollute errors[].
        assert not any(e["item"] == "ghost" for e in row["errors"])
        # All three rows in Films: two real + one tombstone.
        films_by_slug = {f["film_slug"]: f for f in sb.tables["Films"]}
        assert set(films_by_slug) == {"good", "ghost", "ugly"}
        assert films_by_slug["ghost"]["url"] == film_ratings.TOMBSTONE_URL
        assert films_by_slug["ghost"]["lb_rating"] is None
        # Counter surfaced on progress for UI/telemetry.
        assert row["progress"]["film_ratings"]["tombstoned"] == 1
        assert row["progress"]["film_ratings"]["processed"] == 3
        # And a log line was written so operators can grep.
        assert "ghost → tombstoned" in row["log_tail"]

    @pytest.mark.asyncio
    async def test_non_404_errors_still_recorded(self, sb, monkeypatch):
        # Sibling check: only ResourceNotFoundError gets the tombstone treatment;
        # other exceptions still flow to errors[].
        sb.rpcs["get_missing_films"] = lambda: ["good", "broken"]

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                if slug == "broken":
                    raise ValueError("parse failed")
                self.slug, self.url, self.title, self.rating = slug, "u", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert any(e["item"] == "broken" for e in row["errors"])
        # 'broken' did NOT get tombstoned (no Films row written for it).
        assert all(f["film_slug"] != "broken" for f in sb.tables["Films"])
        assert row["progress"]["film_ratings"]["tombstoned"] == 0


class TestPerItemFailures:
    @pytest.mark.asyncio
    async def test_per_user_scrape_failure_continues_with_next_user(self, sb, monkeypatch):
        class FakeUser:
            def __init__(self, lbusername):
                self.lbusername = lbusername

            def get_films(self):
                if self.lbusername == "alice":
                    raise RuntimeError("alice broke")
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                self.slug, self.url, self.title, self.rating = slug, "", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        # Job still completed; alice recorded as error; bob processed normally.
        assert row["status"] == "completed"
        assert any(e["item"] == "alice" and e["phase"] == "user_scrape" for e in row["errors"])
        assert row["progress"]["user_scrape"]["processed"] == 2  # both were attempted

    @pytest.mark.asyncio
    async def test_per_film_failure_continues_and_records_error(self, sb, monkeypatch):
        sb.rpcs["get_missing_films"] = lambda: ["good", "bad", "ugly"]

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                if slug == "bad":
                    raise ValueError("scrape failed")
                self.slug, self.url, self.title, self.rating = slug, "", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        assert any(e["item"] == "bad" for e in row["errors"])
        # 2 of 3 actually upserted to Films.
        assert len(sb.tables["Films"]) == 2


class TestBlockHandling:
    @pytest.fixture(autouse=True)
    def _no_retry(self, monkeypatch):
        # Disable throttle retries so a block propagates immediately (no waits).
        monkeypatch.setattr(letterboxd_throttle, "_RETRY_BACKOFF_SECONDS", ())

    @pytest.mark.asyncio
    async def test_block_fails_fast_with_reason(self, sb, monkeypatch):
        from letterboxdpy.core.exceptions import AccessDeniedError

        scraped: list[str] = []

        class FakeUser:
            def __init__(self, lbusername):
                scraped.append(lbusername)

            def get_films(self):
                raise AccessDeniedError("IP blocked")

        monkeypatch.setattr(user_films, "User", FakeUser)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)

        assert row["status"] == "failed"
        blocked = [e for e in row["errors"] if e.get("reason") == "letterboxd_blocked"]
        assert len(blocked) == 1
        assert blocked[0]["phase"] == "user_scrape"
        # Fail-fast: aborted on the first user, never reached bob or phase 3.
        assert scraped == ["alice"]
        assert sb.tables["Films"] == []

    @pytest.mark.asyncio
    async def test_429_rate_limit_is_treated_as_a_block(self, sb, monkeypatch):
        from letterboxdpy.core.exceptions import InvalidResponseError

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                raise InvalidResponseError("rate limited", code=429)

        monkeypatch.setattr(user_films, "User", FakeUser)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        assert any(e.get("reason") == "letterboxd_blocked" for e in row["errors"])

    @pytest.mark.asyncio
    async def test_block_in_film_ratings_phase_fails(self, sb, monkeypatch):
        # A block can also strike phase 3 (film scrape). Same fail-fast path.
        from letterboxdpy.core.exceptions import AccessDeniedError

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                raise AccessDeniedError("IP blocked")

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        blocked = [e for e in row["errors"] if e.get("reason") == "letterboxd_blocked"]
        assert len(blocked) == 1
        assert blocked[0]["phase"] == "film_ratings"

    @pytest.mark.asyncio
    async def test_private_profile_is_not_a_block(self, sb, monkeypatch):
        # PrivateRouteError is a per-user state, not an IP block: record + continue.
        from letterboxdpy.core.exceptions import PrivateRouteError

        class FakeUser:
            def __init__(self, lbusername):
                self.lbusername = lbusername

            def get_films(self):
                if self.lbusername == "alice":
                    raise PrivateRouteError("alice is private")
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                self.slug, self.url, self.title, self.rating = slug, "u", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        alice_err = next(e for e in row["errors"] if e["item"] == "alice")
        assert "reason" not in alice_err


class TestDefenseInDepth:
    @pytest.mark.asyncio
    async def test_another_running_job_causes_fail(self, sb, monkeypatch):
        _stub_letterboxdpy(monkeypatch)
        # Simulate a stale 'running' row from a previous crashed run.
        sb.insert_refresh_job(uuid4(), status="running")
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        assert any("another job" in e["error"].lower() for e in row["errors"])
        # Confirm we did NOT scrape — failed early.
        assert sb.tables["UserFilms"] == []
        assert sb.tables["Films"] == []


class TestStructuralFailure:
    @pytest.mark.asyncio
    async def test_orchestrator_marks_failed_on_unhandled_exception(self, sb, monkeypatch):
        # Make fetch_users blow up with a structural error (DB unreachable).
        from app.pipeline import users as users_mod

        def boom(_):
            raise ConnectionError("supabase unreachable")

        monkeypatch.setattr(users_mod, "fetch_users", boom)
        # Note: we have to patch where orchestrator imported it.
        monkeypatch.setattr(orchestrator, "fetch_users", boom)

        _stub_letterboxdpy(monkeypatch)
        await orchestrator.run(sb, JOB_ID, table="refresh_jobs")
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        assert any("ConnectionError" in e["error"] for e in row["errors"])


# ---- Per-user mode (table='user_scrape_jobs', lbusername set) ---------------


@pytest.fixture
def sb_user_scope() -> FakeSupabase:
    """A FakeSupabase preloaded with one user_scrape_jobs row for alice."""
    s = FakeSupabase()
    s.insert_refresh_job(JOB_ID, status="running", table="user_scrape_jobs", lbusername="alice")
    s.insert_users("alice", "bob", is_discord=True)
    return s


class TestPerUserScope:
    @pytest.mark.asyncio
    async def test_only_target_user_is_scraped(self, sb_user_scope, monkeypatch):
        """Phase 1 must touch alice only, not bob, even though bob is in Users."""
        scraped_users: list[str] = []

        class FakeUser:
            def __init__(self, lbusername):
                self.lbusername = lbusername
                scraped_users.append(lbusername)

            def get_films(self):
                return {"movies": {"parasite": {"rating": 4.5, "liked": True, "name": "Parasite"}}}

        class FakeMovie:
            def __init__(self, slug):
                self.slug, self.url, self.title, self.rating = slug, "u", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)
        sb_user_scope.rpcs["get_missing_films_for_user"] = lambda params: ["parasite"]

        await orchestrator.run(
            sb_user_scope, JOB_ID, table="user_scrape_jobs", lbusername="alice"
        )

        assert scraped_users == ["alice"]
        row = sb_user_scope.get_refresh_job(JOB_ID, table="user_scrape_jobs")
        assert row["status"] == "completed"

    @pytest.mark.asyncio
    async def test_phase2_uses_scoped_rpc(self, sb_user_scope, monkeypatch):
        """Phase 2 calls get_missing_films_for_user, not get_missing_films."""
        captured_params: list[dict] = []

        def scoped_rpc(params):
            captured_params.append(params)
            return ["alice-only-slug"]

        sb_user_scope.rpcs["get_missing_films_for_user"] = scoped_rpc
        # Tripwire: bulk RPC should NOT be called in per-user mode.
        sb_user_scope.rpcs["get_missing_films"] = lambda: pytest.fail(
            "bulk get_missing_films RPC must not be called in per-user mode"
        )

        class FakeUser:
            def __init__(self, _):
                pass

            def get_films(self):
                return {"movies": {}}

        class FakeMovie:
            def __init__(self, slug):
                self.slug, self.url, self.title, self.rating = slug, "u", slug, 4.0
                self.tmdb_link = self.poster = self.banner = None

        monkeypatch.setattr(user_films, "User", FakeUser)
        monkeypatch.setattr(film_ratings, "Movie", FakeMovie)

        await orchestrator.run(
            sb_user_scope, JOB_ID, table="user_scrape_jobs", lbusername="alice"
        )

        assert captured_params == [{"p_lbusername": "alice"}]
        # The slug returned by the scoped RPC actually got upserted to Films.
        assert any(f["film_slug"] == "alice-only-slug" for f in sb_user_scope.tables["Films"])

    @pytest.mark.asyncio
    async def test_writes_go_to_user_scrape_jobs_not_refresh_jobs(self, sb_user_scope, monkeypatch):
        """JobState's parameterized table routes all updates to user_scrape_jobs."""
        _stub_letterboxdpy(monkeypatch)
        sb_user_scope.rpcs["get_missing_films_for_user"] = lambda params: []

        await orchestrator.run(
            sb_user_scope, JOB_ID, table="user_scrape_jobs", lbusername="alice"
        )

        # All update writes from this job target user_scrape_jobs.
        update_tables = {w["table"] for w in sb_user_scope.writes if w["op"] == "update"}
        assert update_tables == {"user_scrape_jobs"}

    @pytest.mark.asyncio
    async def test_does_not_check_another_job_running(self, sb_user_scope, monkeypatch):
        """Per-user must NOT trip the global another-job-running defense.
        Two different per-user jobs (different usernames) are allowed to run
        concurrently — single-flight is per-username at SQL."""
        _stub_letterboxdpy(monkeypatch)
        sb_user_scope.rpcs["get_missing_films_for_user"] = lambda params: []
        # Seed a *second* per-user job (bob's, also running) and assert our
        # alice job completes anyway.
        other_job = uuid4()
        sb_user_scope.insert_refresh_job(
            other_job, status="running", table="user_scrape_jobs", lbusername="bob"
        )

        await orchestrator.run(
            sb_user_scope, JOB_ID, table="user_scrape_jobs", lbusername="alice"
        )

        row = sb_user_scope.get_refresh_job(JOB_ID, table="user_scrape_jobs")
        assert row["status"] == "completed"
        # The other running job is untouched.
        other = sb_user_scope.get_refresh_job(other_job, table="user_scrape_jobs")
        assert other["status"] == "running"

    @pytest.mark.asyncio
    async def test_progress_shape_matches_bulk(self, sb_user_scope, monkeypatch):
        """All 3 phases left progress on the per-user job, same shape as bulk
        — the client UI can render the same <JobProgress> component for both."""
        _stub_letterboxdpy(
            monkeypatch,
            user_films_per_user={
                "alice": {"movies": {"parasite": {"rating": 4.5, "liked": True, "name": "Parasite"}}},
            },
        )
        sb_user_scope.rpcs["get_missing_films_for_user"] = lambda params: ["parasite"]

        await orchestrator.run(
            sb_user_scope, JOB_ID, table="user_scrape_jobs", lbusername="alice"
        )

        row = sb_user_scope.get_refresh_job(JOB_ID, table="user_scrape_jobs")
        assert row["progress"]["user_scrape"]["processed"] == 1
        assert row["progress"]["user_scrape"]["total"] == 1
        assert row["progress"]["missing_films"]["count"] == 1
        assert row["progress"]["film_ratings"]["processed"] == 1
        assert row["progress"]["film_ratings"]["total"] == 1
