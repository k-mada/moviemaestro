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

from app.pipeline import film_ratings, orchestrator, user_films
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
        await orchestrator.run(sb, JOB_ID)
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
        await orchestrator.run(sb, JOB_ID)
        row = sb.get_refresh_job(JOB_ID)
        assert row["progress"]["user_scrape"]["processed"] == 2
        assert row["progress"]["user_scrape"]["total"] == 2
        assert row["progress"]["missing_films"]["count"] == 2
        assert row["progress"]["film_ratings"]["processed"] == 2
        assert row["progress"]["film_ratings"]["total"] == 2

    @pytest.mark.asyncio
    async def test_phase_order_in_log(self, sb, monkeypatch):
        _stub_letterboxdpy(monkeypatch)
        await orchestrator.run(sb, JOB_ID)
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
        await orchestrator.run(sb, JOB_ID)
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

        await orchestrator.run(sb, JOB_ID)
        row = sb.get_refresh_job(JOB_ID)
        # Bailed mid-phase — not all 5 slugs processed.
        assert len(scraped) < 5
        assert row["status"] == "cancelled"
        assert row["finished_at"] is not None


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

        await orchestrator.run(sb, JOB_ID)
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

        await orchestrator.run(sb, JOB_ID)
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "completed"
        assert any(e["item"] == "bad" for e in row["errors"])
        # 2 of 3 actually upserted to Films.
        assert len(sb.tables["Films"]) == 2


class TestDefenseInDepth:
    @pytest.mark.asyncio
    async def test_another_running_job_causes_fail(self, sb, monkeypatch):
        _stub_letterboxdpy(monkeypatch)
        # Simulate a stale 'running' row from a previous crashed run.
        sb.insert_refresh_job(uuid4(), status="running")
        await orchestrator.run(sb, JOB_ID)
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
        await orchestrator.run(sb, JOB_ID)
        row = sb.get_refresh_job(JOB_ID)
        assert row["status"] == "failed"
        assert any("ConnectionError" in e["error"] for e in row["errors"])
