"""Tests for pipeline leaf modules: users, missing, user_films, film_ratings.

We patch letterboxdpy at the import-site (e.g. app.pipeline.user_films.User)
so the real network calls never fire.
"""

from __future__ import annotations

import pytest

from app.pipeline import film_ratings, user_films
from app.pipeline.missing import get_missing_film_slugs
from app.pipeline.users import fetch_users
from tests.fakes import FakeSupabase


@pytest.fixture
def sb() -> FakeSupabase:
    return FakeSupabase()


# ---------- users ----------

class TestFetchUsers:
    def test_returns_only_discord_users(self, sb):
        sb.insert_users("alice", "bob", is_discord=True)
        sb.insert_users("lurker", is_discord=False)
        result = fetch_users(sb)
        assert sorted(result) == ["alice", "bob"]

    def test_empty_table_returns_empty_list(self, sb):
        assert fetch_users(sb) == []

    def test_strips_leading_and_trailing_whitespace(self, sb):
        # Stray whitespace from manual SQL edits would otherwise crash
        # letterboxdpy.User() with AssertionError("Invalid username").
        sb.tables["Users"].extend([
            {"lbusername": " jonleblanc", "is_discord": True},
            {"lbusername": "alice ", "is_discord": True},
            {"lbusername": "\tbob\n", "is_discord": True},
            # NBSP is U+00A0; Python's str.strip() handles it.
            {"lbusername": " carol ", "is_discord": True},
        ])
        result = fetch_users(sb)
        assert sorted(result) == ["alice", "bob", "carol", "jonleblanc"]

    def test_drops_empty_or_whitespace_only_usernames(self, sb):
        sb.tables["Users"].extend([
            {"lbusername": "alice", "is_discord": True},
            {"lbusername": "", "is_discord": True},
            {"lbusername": "   ", "is_discord": True},
            {"lbusername": None, "is_discord": True},
        ])
        result = fetch_users(sb)
        assert result == ["alice"]


# ---------- missing ----------

class TestGetMissingFilmSlugs:
    def test_returns_rpc_data(self, sb):
        sb.rpcs["get_missing_films"] = lambda: ["a", "b", "c"]
        assert get_missing_film_slugs(sb) == ["a", "b", "c"]

    def test_handles_null_data(self, sb):
        sb.rpcs["get_missing_films"] = lambda: None
        assert get_missing_film_slugs(sb) == []


# ---------- user_films ----------

class _FakeUser:
    """Minimal stand-in for letterboxdpy.user.User."""

    def __init__(self, lbusername: str, films_dict: dict | None = None) -> None:
        self.lbusername = lbusername
        self._films = films_dict or {"movies": {}}

    def get_films(self) -> dict:
        return self._films


class TestScrapeAndUpsertUserFilms:
    def test_upserts_all_movies_with_ratings_and_likes(self, sb, monkeypatch):
        films = {
            "movies": {
                "parasite": {"rating": 4.5, "liked": True, "name": "Parasite"},
                "the-room": {"rating": 0.5, "liked": False, "name": "The Room"},
                "unrated": {"rating": None, "liked": False, "name": "Unrated"},
            }
        }
        monkeypatch.setattr(
            user_films, "User", lambda lbusername: _FakeUser(lbusername, films)
        )
        count = user_films.scrape_and_upsert_user_films(sb, "alice")
        assert count == 3
        # Verify rows landed in UserFilms with the right shape.
        rows = sb.tables["UserFilms"]
        assert len(rows) == 3
        parasite = next(r for r in rows if r["film_slug"] == "parasite")
        assert parasite["rating"] == 4.5
        assert parasite["liked"] is True
        assert parasite["title"] == "Parasite"
        assert parasite["lbusername"] == "alice"
        # Unrated film stored with rating=None — allowed by schema.
        unrated = next(r for r in rows if r["film_slug"] == "unrated")
        assert unrated["rating"] is None

    def test_empty_movies_dict_returns_zero_no_upsert(self, sb, monkeypatch):
        monkeypatch.setattr(user_films, "User", lambda u: _FakeUser(u, {"movies": {}}))
        count = user_films.scrape_and_upsert_user_films(sb, "ghost")
        assert count == 0
        assert sb.tables["UserFilms"] == []

    def test_letterboxdpy_exception_propagates(self, sb, monkeypatch):
        def boom(_):
            raise RuntimeError("cloudflare")
        monkeypatch.setattr(user_films, "User", boom)
        with pytest.raises(RuntimeError, match="cloudflare"):
            user_films.scrape_and_upsert_user_films(sb, "alice")

    def test_upsert_uses_composite_pk_conflict_target(self, sb, monkeypatch):
        # Re-running the same user replaces rows rather than duplicating.
        films = {"movies": {"parasite": {"rating": 4.5, "liked": True, "name": "Parasite"}}}
        monkeypatch.setattr(user_films, "User", lambda u: _FakeUser(u, films))
        user_films.scrape_and_upsert_user_films(sb, "alice")
        # Now the rating changes.
        films["movies"]["parasite"]["rating"] = 5.0
        user_films.scrape_and_upsert_user_films(sb, "alice")
        rows = [r for r in sb.tables["UserFilms"] if r["film_slug"] == "parasite"]
        assert len(rows) == 1
        assert rows[0]["rating"] == 5.0

    def test_writes_user_ratings_histogram(self, sb, monkeypatch):
        """The aggregated rating-level histogram lands in UserRatings.

        Today's bulk path never touched UserRatings — the legacy Vercel
        cron was the only writer. Now moviemaestro keeps the histogram
        fresh for both bulk and per-user flows, so /fetcher users see
        up-to-date averages even if the cron is broken or skipped.
        """
        films = {
            "movies": {
                "a": {"rating": 4.5, "liked": True, "name": "A"},
                "b": {"rating": 4.5, "liked": False, "name": "B"},
                "c": {"rating": 4.0, "liked": False, "name": "C"},
                "d": {"rating": None, "liked": False, "name": "D"},  # unrated, ignored
            }
        }
        monkeypatch.setattr(user_films, "User", lambda u: _FakeUser(u, films))
        user_films.scrape_and_upsert_user_films(sb, "alice")

        ratings = sorted(sb.tables["UserRatings"], key=lambda r: r["rating"])
        # Two distinct rating levels; unrated film is not counted.
        assert ratings == [
            {"username": "alice", "rating": 4.0, "count": 1},
            {"username": "alice", "rating": 4.5, "count": 2},
        ]

    def test_user_ratings_upsert_overwrites_on_rescrape(self, sb, monkeypatch):
        """A second scrape with updated ratings replaces the level counts."""
        films = {"movies": {"a": {"rating": 4.5, "liked": False, "name": "A"}}}
        monkeypatch.setattr(user_films, "User", lambda u: _FakeUser(u, films))
        user_films.scrape_and_upsert_user_films(sb, "alice")
        assert sb.tables["UserRatings"] == [
            {"username": "alice", "rating": 4.5, "count": 1}
        ]
        # User adds a second 4.5-star rating.
        films["movies"]["b"] = {"rating": 4.5, "liked": False, "name": "B"}
        user_films.scrape_and_upsert_user_films(sb, "alice")
        ratings = [r for r in sb.tables["UserRatings"] if r["username"] == "alice"]
        assert len(ratings) == 1
        assert ratings[0]["count"] == 2

    def test_no_user_ratings_written_when_user_has_no_rated_films(self, sb, monkeypatch):
        """A user with films but zero rated entries gets no histogram rows."""
        films = {
            "movies": {
                "a": {"rating": None, "liked": False, "name": "A"},
                "b": {"rating": None, "liked": False, "name": "B"},
            }
        }
        monkeypatch.setattr(user_films, "User", lambda u: _FakeUser(u, films))
        user_films.scrape_and_upsert_user_films(sb, "alice")
        # UserFilms got the two unrated rows, UserRatings got nothing.
        assert len(sb.tables["UserFilms"]) == 2
        assert sb.tables.get("UserRatings", []) == []


# ---------- film_ratings ----------

class _FakeMovie:
    def __init__(self, slug: str) -> None:
        self.slug = slug
        self.url = f"https://letterboxd.com/film/{slug}/"
        self.title = slug.replace("-", " ").title()
        self.rating = 4.0
        self.tmdb_link = f"https://themoviedb.org/movie/{slug}"
        self.poster = None
        self.banner = None
        self.year = 2020


def _make_fake_movie(year: int | None = 2020):
    def _factory(slug: str) -> _FakeMovie:
        m = _FakeMovie(slug)
        m.year = year
        return m
    return _factory


class TestScrapeAndUpsertFilm:
    def test_upserts_film_with_metadata(self, sb, monkeypatch):
        monkeypatch.setattr(film_ratings, "Movie", _FakeMovie)
        film_ratings.scrape_and_upsert_film(sb, "parasite")
        rows = sb.tables["Films"]
        assert len(rows) == 1
        assert rows[0]["film_slug"] == "parasite"
        assert rows[0]["lb_rating"] == 4.0
        assert rows[0]["title"] == "Parasite"
        assert rows[0]["release_year"] == 2020

    def test_null_release_year_does_not_clobber_existing(self, sb, monkeypatch):
        # First scrape lands release_year=2010. Second scrape, where year
        # parsing fails (Movie.year is None), must NOT overwrite 2010 — the
        # COALESCE in upsert_film() owns this invariant.
        monkeypatch.setattr(film_ratings, "Movie", _make_fake_movie(year=2010))
        film_ratings.scrape_and_upsert_film(sb, "inception")
        assert sb.tables["Films"][0]["release_year"] == 2010

        monkeypatch.setattr(film_ratings, "Movie", _make_fake_movie(year=None))
        film_ratings.scrape_and_upsert_film(sb, "inception")
        rows = [r for r in sb.tables["Films"] if r["film_slug"] == "inception"]
        assert len(rows) == 1
        assert rows[0]["release_year"] == 2010

    def test_letterboxdpy_exception_propagates(self, sb, monkeypatch):
        def boom(_):
            raise ValueError("invalid slug")
        monkeypatch.setattr(film_ratings, "Movie", boom)
        with pytest.raises(ValueError):
            film_ratings.scrape_and_upsert_film(sb, "bad-slug")

    def test_repeated_scrape_dedupes_on_film_slug(self, sb, monkeypatch):
        monkeypatch.setattr(film_ratings, "Movie", _FakeMovie)
        film_ratings.scrape_and_upsert_film(sb, "parasite")
        film_ratings.scrape_and_upsert_film(sb, "parasite")
        assert len(sb.tables["Films"]) == 1


class TestTombstoneFilm:
    def test_writes_stub_row_with_marker_url(self, sb):
        film_ratings.tombstone_film(sb, "blank-check-on-broadway")
        rows = sb.tables["Films"]
        assert len(rows) == 1
        assert rows[0]["film_slug"] == "blank-check-on-broadway"
        assert rows[0]["url"] == film_ratings.TOMBSTONE_URL
        assert rows[0]["lb_rating"] is None

    def test_tombstone_dedupes_on_film_slug(self, sb):
        film_ratings.tombstone_film(sb, "ghost-slug")
        film_ratings.tombstone_film(sb, "ghost-slug")
        assert len(sb.tables["Films"]) == 1
