"""Phase 3 helpers: scrape one film's Letterboxd average rating + metadata, upsert into Films."""

from __future__ import annotations

from letterboxdpy.movie import Movie
from supabase import Client

# Marker stored in Films.url for slugs that 404'd on Letterboxd. Distinguishable
# from a real Letterboxd URL by inspection or `WHERE url = 'unreachable'`.
TOMBSTONE_URL = "unreachable"


def scrape_and_upsert_film(supabase: Client, slug: str) -> None:
    """Scrape one Letterboxd film and upsert into Films.

    Raises whatever letterboxdpy raises on failure — caller catches and records.
    """
    movie = Movie(slug)
    row = {
        "film_slug": movie.slug,
        "url": movie.url,
        "title": movie.title,
        "lb_rating": movie.rating,
        "tmdb_link": getattr(movie, "tmdb_link", None),
        "poster": getattr(movie, "poster", None),
        "banner": getattr(movie, "banner", None),
    }
    supabase.table("Films").upsert(row, on_conflict="film_slug").execute()


def tombstone_film(supabase: Client, slug: str) -> None:
    """Insert a stub Films row to suppress future retries of a 404'd slug.

    get_missing_films() returns UserFilms slugs without a matching Films row,
    so a stub row — even with lb_rating NULL — keeps the slug out of future
    runs. Manual retry: DELETE FROM "Films" WHERE film_slug = '...'.
    """
    supabase.table("Films").upsert(
        {"film_slug": slug, "url": TOMBSTONE_URL, "lb_rating": None},
        on_conflict="film_slug",
    ).execute()
