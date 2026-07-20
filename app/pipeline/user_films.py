"""Phase 1 helpers: scrape one user's rated films via letterboxdpy and upsert."""

from __future__ import annotations

from typing import Any

from letterboxdpy.user import User
from supabase import Client

from app.pipeline.user_ratings import refresh_user_ratings


def scrape_and_upsert_user_films(supabase: Client, lbusername: str) -> int:
    """Scrape `lbusername`'s film grid, upsert into UserFilms, and refresh
    the user's rating histogram in UserRatings.

    Returns the number of UserFilms rows upserted. Raises whatever
    letterboxdpy raises on fetch failure — the caller catches and records.
    """
    user = User(lbusername)
    films = user.get_films()  # {"movies": {slug: {...}}, "count": ..., ...}

    film_rows: list[dict[str, Any]] = [
        {
            "lbusername": lbusername,
            "film_slug": slug,
            "rating": movie.get("rating"),
            "liked": bool(movie.get("liked")),
            "title": movie.get("name") or movie.get("title"),
        }
        for slug, movie in (films.get("movies") or {}).items()
    ]

    if not film_rows:
        return 0

    # Conflict target matches the composite PK (lbusername, film_slug).
    supabase.table("UserFilms").upsert(
        film_rows, on_conflict="lbusername,film_slug"
    ).execute()

    # Recompute the histogram from the persisted rows — one path shared with RSS.
    refresh_user_ratings(supabase, lbusername)

    return len(film_rows)
