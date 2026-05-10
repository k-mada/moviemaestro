"""Phase 1 helpers: scrape one user's rated films via letterboxdpy and upsert."""

from __future__ import annotations

from typing import Any

from letterboxdpy.user import User
from supabase import Client


def scrape_and_upsert_user_films(supabase: Client, lbusername: str) -> int:
    """Scrape `lbusername`'s film grid and upsert into UserFilms.

    Returns the number of rows upserted. Raises whatever letterboxdpy raises on
    fetch failure — the caller is responsible for catching and recording.
    """
    user = User(lbusername)
    films = user.get_films()  # {"movies": {slug: {...}}, "count": ..., ...}

    rows: list[dict[str, Any]] = []
    for slug, movie in (films.get("movies") or {}).items():
        rows.append(
            {
                "lbusername": lbusername,
                "film_slug": slug,
                "rating": movie.get("rating"),
                "liked": bool(movie.get("liked")),
                "title": movie.get("name") or movie.get("title"),
            }
        )

    if not rows:
        return 0

    # Single batched upsert. Conflict target matches the table's composite PK
    # (lbusername, film_slug); existing rows are overwritten.
    supabase.table("UserFilms").upsert(
        rows, on_conflict="lbusername,film_slug"
    ).execute()
    return len(rows)
