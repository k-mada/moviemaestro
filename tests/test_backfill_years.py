"""Tests for app/pipeline/backfill_years.py.

Covers the leaf module's contract: cursor pagination shape, year validation,
the concurrent-writer guard, dry_run, failure paths, and idempotency. The
HTTP layer is tested separately in tests/test_backfill_endpoint.py.
"""

from __future__ import annotations

import pytest

from app.pipeline import backfill_years
from tests.fakes import FakeSupabase


@pytest.fixture
def sb() -> FakeSupabase:
    return FakeSupabase()


def _insert_film(sb: FakeSupabase, slug: str, *, release_year=None, updated_at=None) -> dict:
    row = {
        "film_slug": slug,
        "title": slug.replace("-", " ").title(),
        "lb_rating": 3.5,
        "url": f"https://letterboxd.com/film/{slug}/",
        "release_year": release_year,
    }
    if updated_at is not None:
        row["updated_at"] = updated_at
    sb.tables["Films"].append(row)
    return row


class _FakeMovie:
    def __init__(self, slug: str, *, year: int | None = 2020) -> None:
        self.slug = slug
        self.year = year


def _movie_factory(year: int | None = 2020):
    def _f(slug: str) -> _FakeMovie:
        return _FakeMovie(slug, year=year)
    return _f


async def _run(sb, **kwargs):
    return await backfill_years.run_batch(
        sb,
        batch_size=kwargs.pop("batch_size", 100),
        after_slug=kwargs.pop("after_slug", None),
        dry_run=kwargs.pop("dry_run", False),
    )


class TestHappyPath:
    @pytest.mark.asyncio
    async def test_populates_release_year_for_null_rows(self, sb, monkeypatch):
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=1994))
        _insert_film(sb, "pulp-fiction")

        result = await _run(sb)

        assert result.processed == 1
        assert result.updated == 1
        assert result.failures == []
        row = sb.tables["Films"][0]
        assert row["release_year"] == 1994

    @pytest.mark.asyncio
    async def test_does_not_bump_updated_at(self, sb, monkeypatch):
        # Spec acceptance: backfill is bookkeeping, not a content update.
        # updated_at must stay where it was so consumers' freshness signal
        # isn't polluted with backfill noise.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2000))
        sentinel = "2024-01-01T00:00:00Z"
        _insert_film(sb, "stalker", updated_at=sentinel)

        await _run(sb)

        assert sb.tables["Films"][0]["updated_at"] == sentinel
        # And the write itself shouldn't have included updated_at.
        update_writes = [w for w in sb.writes if w["op"] == "update"]
        assert update_writes
        assert "updated_at" not in update_writes[-1]["payload"]

    @pytest.mark.asyncio
    async def test_only_null_release_year_rows_selected(self, sb, monkeypatch):
        # A row with release_year already populated must not enter the SELECT.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        _insert_film(sb, "a-null-film")
        _insert_film(sb, "b-already-set", release_year=2010)

        result = await _run(sb)

        assert result.processed == 1
        already_set = next(r for r in sb.tables["Films"] if r["film_slug"] == "b-already-set")
        assert already_set["release_year"] == 2010


class TestYearValidation:
    @pytest.mark.asyncio
    async def test_year_none_is_silent_skip(self, sb, monkeypatch):
        # Letterboxd page loaded but no year — not a failure, row stays NULL.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=None))
        _insert_film(sb, "yearless")

        result = await _run(sb)

        assert result.processed == 1
        assert result.updated == 0
        assert result.failures == []
        assert sb.tables["Films"][0]["release_year"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_year", [0, 1500, 1887, 9999, "2020", 2020.5])
    async def test_implausible_year_recorded_as_failure(self, sb, monkeypatch, bad_year):
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=bad_year))
        _insert_film(sb, "bogus")

        result = await _run(sb)

        assert result.updated == 0
        assert len(result.failures) == 1
        assert result.failures[0]["film_slug"] == "bogus"
        assert "implausible" in result.failures[0]["error"]
        assert sb.tables["Films"][0]["release_year"] is None


class TestFailures:
    @pytest.mark.asyncio
    async def test_movie_exception_recorded_with_class_name(self, sb, monkeypatch):
        def boom(slug):
            raise RuntimeError("cloudflare blocked")
        monkeypatch.setattr(backfill_years, "Movie", boom)
        _insert_film(sb, "blocked")

        result = await _run(sb)

        assert result.processed == 1
        assert result.updated == 0
        assert len(result.failures) == 1
        assert result.failures[0]["film_slug"] == "blocked"
        assert result.failures[0]["error"] == "RuntimeError: cloudflare blocked"

    @pytest.mark.asyncio
    async def test_supabase_write_exception_does_not_kill_batch(self, sb, monkeypatch):
        # Mid-batch Postgres error must surface in failures[] without
        # truncating subsequent rows.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        _insert_film(sb, "a-bad-write")
        _insert_film(sb, "b-good")
        _insert_film(sb, "c-good")

        real_update = sb.table

        def patched_table(name):
            t = real_update(name)
            real_execute = t.execute

            def execute():
                # Detect the update call against the offending slug.
                if (
                    t._action == "update"
                    and any(f.col == "film_slug" and f.val == "a-bad-write" for f in t._filters)
                ):
                    raise ConnectionError("supabase blew up")
                return real_execute()

            t.execute = execute
            return t

        monkeypatch.setattr(sb, "table", patched_table)

        result = await _run(sb)

        assert result.processed == 3
        assert result.updated == 2
        assert len(result.failures) == 1
        assert result.failures[0]["film_slug"] == "a-bad-write"
        assert "ConnectionError" in result.failures[0]["error"]
        # Subsequent rows were still committed.
        good_rows = {r["film_slug"]: r for r in sb.tables["Films"]}
        assert good_rows["b-good"]["release_year"] == 2020
        assert good_rows["c-good"]["release_year"] == 2020
        # And the offender stayed NULL.
        assert good_rows["a-bad-write"]["release_year"] is None


class TestGuardRace:
    @pytest.mark.asyncio
    async def test_concurrent_writer_blocks_update(self, sb, monkeypatch):
        # Simulate the race the AND release_year IS NULL guard exists for:
        # a Stage-2 upsert lands on the slug between our SELECT and our
        # UPDATE, populating release_year. Our UPDATE must skip — not
        # overwrite — that value.
        _insert_film(sb, "raced")

        def racing_movie(slug):
            # Concurrent writer mutates the row to 1955 BEFORE our UPDATE
            # fires (we are still in the "scrape" half of the loop).
            sb.tables["Films"][0]["release_year"] = 1955
            return _FakeMovie(slug, year=2020)

        monkeypatch.setattr(backfill_years, "Movie", racing_movie)

        result = await _run(sb)

        assert result.processed == 1
        assert result.updated == 0, "guard should have rejected the UPDATE"
        # Guard rejection is the expected steady-state; it must not surface
        # as a failure to the caller. Stage 4b would otherwise treat normal
        # concurrent-writer races as ops-actionable noise.
        assert result.failures == []
        # The concurrent writer's value survived.
        assert sb.tables["Films"][0]["release_year"] == 1955


class TestDryRun:
    @pytest.mark.asyncio
    async def test_dry_run_does_not_write_but_counts_intended_updates(self, sb, monkeypatch):
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        _insert_film(sb, "preview-1")
        _insert_film(sb, "preview-2")

        result = await _run(sb, dry_run=True)

        assert result.processed == 2
        assert result.updated == 2
        assert all(r["release_year"] is None for r in sb.tables["Films"])
        # No update writes were issued.
        assert not any(w["op"] == "update" for w in sb.writes)

    @pytest.mark.asyncio
    async def test_dry_run_still_surfaces_failures(self, sb, monkeypatch):
        # The whole point of dry_run is pipeline validation, so scrape
        # failures must still appear so the operator can triage before a
        # live run.
        def boom(slug):
            raise RuntimeError("nope")
        monkeypatch.setattr(backfill_years, "Movie", boom)
        _insert_film(sb, "broken")

        result = await _run(sb, dry_run=True)

        assert result.failures and result.failures[0]["film_slug"] == "broken"


class TestCursor:
    @pytest.mark.asyncio
    async def test_after_slug_uses_strict_greater_than(self, sb, monkeypatch):
        # Boundary slug from previous page must NOT be re-scraped.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        for slug in ["aaa", "bbb", "ccc", "ddd"]:
            _insert_film(sb, slug)

        result = await _run(sb, after_slug="bbb", batch_size=10)

        # bbb is the cursor; only ccc and ddd should appear.
        assert result.processed == 2
        assert result.updated == 2
        bbb = next(r for r in sb.tables["Films"] if r["film_slug"] == "bbb")
        assert bbb["release_year"] is None

    @pytest.mark.asyncio
    async def test_next_after_slug_is_last_slug_when_page_full(self, sb, monkeypatch):
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        for slug in ["a", "b", "c"]:
            _insert_film(sb, slug)

        result = await _run(sb, batch_size=2)

        assert result.processed == 2
        assert result.next_after_slug == "b"

    @pytest.mark.asyncio
    async def test_next_after_slug_none_when_page_partial(self, sb, monkeypatch):
        # Partial page signals the sweep is exhausted from the current cursor.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        for slug in ["a", "b"]:
            _insert_film(sb, slug)

        result = await _run(sb, batch_size=10)

        assert result.processed == 2
        assert result.next_after_slug is None

    @pytest.mark.asyncio
    async def test_next_after_slug_none_when_no_rows(self, sb, monkeypatch):
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))

        result = await _run(sb)

        assert result.processed == 0
        assert result.updated == 0
        assert result.next_after_slug is None

    @pytest.mark.asyncio
    async def test_results_ordered_by_film_slug(self, sb, monkeypatch):
        # The cursor relies on ORDER BY film_slug; verify the SELECT honors it.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        for slug in ["zeta", "alpha", "mu"]:
            _insert_film(sb, slug)

        result = await _run(sb, batch_size=1)

        # First slug alphabetically is "alpha"; cursor advances to it.
        assert result.next_after_slug == "alpha"


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_second_run_is_noop(self, sb, monkeypatch):
        # Spec acceptance: running the same batch twice produces no
        # double-writes and updated == 0 on the second call.
        monkeypatch.setattr(backfill_years, "Movie", _movie_factory(year=2020))
        _insert_film(sb, "once")

        first = await _run(sb)
        second = await _run(sb)

        assert first.updated == 1
        assert second.processed == 0
        assert second.updated == 0
        assert sb.tables["Films"][0]["release_year"] == 2020
