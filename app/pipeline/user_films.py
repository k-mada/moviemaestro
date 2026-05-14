"""Phase 1 helpers: scrape one user's rated films via letterboxdpy and upsert."""

from __future__ import annotations

from typing import Any

from letterboxdpy.user import User
from supabase import Client


def scrape_and_upsert_user_films(supabase: Client, lbusername: str) -> int:
    """Scrape `lbusername`'s film grid, upsert into UserFilms, and refresh
    the user's rating histogram in UserRatings.

    letterboxdpy's get_films() paginates the user's full diary, so the
    histogram is computed directly from the scraped per-film ratings —
    no extra HTTP call to Letterboxd's /ratings/ page. The legacy Vercel
    cron is the only other path that updates UserRatings; by writing here
    we keep the histogram fresh for any user who triggers /fetcher even if
    the cron is broken or skipped.

    Returns the number of UserFilms rows upserted. Raises whatever
    letterboxdpy raises on fetch failure — the caller catches and records.
    """
    user = User(lbusername)
    films = user.get_films()  # {"movies": {slug: {...}}, "count": ..., ...}

    film_rows: list[dict[str, Any]] = []
    # rating → count of films the user has rated at that level (0.5..5.0).
    rating_counts: dict[float, int] = {}
    for slug, movie in (films.get("movies") or {}).items():
        rating = movie.get("rating")
        film_rows.append(
            {
                "lbusername": lbusername,
                "film_slug": slug,
                "rating": rating,
                "liked": bool(movie.get("liked")),
                "title": movie.get("name") or movie.get("title"),
            }
        )
        if rating is not None:
            key = float(rating)
            rating_counts[key] = rating_counts.get(key, 0) + 1

    if not film_rows:
        return 0

    # Single batched upsert. Conflict target matches the table's composite PK
    # (lbusername, film_slug); existing rows are overwritten.
    supabase.table("UserFilms").upsert(
        film_rows, on_conflict="lbusername,film_slug"
    ).execute()

    # Same semantics as bpdiscord's dbUpsertUserRatings: upsert only the
    # levels present in this scrape. A rating level the user used to have
    # but has since cleared will keep its stale count until a future
    # cleanup pass — pre-existing data quality issue, not in scope here.
    if rating_counts:
        rating_rows = [
            {"username": lbusername, "rating": rating, "count": count}
            for rating, count in rating_counts.items()
        ]
        supabase.table("UserRatings").upsert(
            rating_rows, on_conflict="username,rating"
        ).execute()

    return len(film_rows)
