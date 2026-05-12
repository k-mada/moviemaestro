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


class TestScrapeAndUpsertFilm:
    def test_upserts_film_with_metadata(self, sb, monkeypatch):
        monkeypatch.setattr(film_ratings, "Movie", _FakeMovie)
        film_ratings.scrape_and_upsert_film(sb, "parasite")
        rows = sb.tables["Films"]
        assert len(rows) == 1
        assert rows[0]["film_slug"] == "parasite"
        assert rows[0]["lb_rating"] == 4.0
        assert rows[0]["title"] == "Parasite"

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
